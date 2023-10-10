#![warn(missing_docs)]
use std::{
    env, fs,
    fs::{write, File},
    io,
    io::{BufRead, BufReader},
    path::Path,
    process::Command,
};

/// Runs the `forge` command-line tool to generate bindings.
///
/// This function attempts to execute the external command `forge` with the
/// provided arguments to generate necessary bindings. The bindings are stored
/// in the `arbiter/src/bindings/` directory, and existing bindings will be
/// overwritten. The function wraps the forge command to generate bindings as a
/// module to a specific destination.
///
/// # Returns
///
/// * `Ok(())` if the `forge` command successfully generates the bindings.
/// * `Err(std::io::Error)` if the command execution fails or if there's an
///   error in generating the bindings. This can also include if the `forge`
///   tool is not installed.

pub(crate) fn forge_bind() -> std::io::Result<()> {
    println!("Generating bindings for project contracts...");
    let output = Command::new("forge")
        .arg("bind")
        .arg("--revert-strings")
        .arg("debug")
        .arg("-b")
        .arg("src/bindings/")
        .arg("--module")
        .arg("--overwrite")
        .output()?;
    let project_contracts = collect_contract_list(Path::new("contracts"))?;
    if output.status.success() {
        let output_str = String::from_utf8_lossy(&output.stdout);
        println!("Command output: {}", output_str);
        println!("Revert strings are on");
    } else {
        let err_str = String::from_utf8_lossy(&output.stderr);
        println!("Command failed, error: {}, is forge installed?", err_str);
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Command failed",
        ));
    }

    let src_binding_dir = Path::new("src/bindings");
    remove_unneeded_contracts(src_binding_dir, project_contracts)?;

    let lib_dir = Path::new("lib");
    let (output_path, sub_module_contracts) = bindings_for_submodules(lib_dir)?;
    println!("submodule contracts: {:?}", sub_module_contracts);
    remove_unneeded_contracts(Path::new(&output_path), sub_module_contracts)?;

    Ok(())
}

fn bindings_for_submodules(dir: &Path) -> io::Result<(String, Vec<String>)> {
    let mut contracts_to_generate = Vec::new(); // to keep track of contracts we're generating bindings for
    let mut output_path = String::new();
    if dir.is_dir() {
        // Iterate through entries in the directory
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            // If the entry is a directory, run command inside it
            if path.is_dir() && path.file_name().unwrap_or_default() != "forge-std" {
                contracts_to_generate = collect_contract_list(&path)?;
                println!("Generating bindings for submodule: {:?}...", path);

                env::set_current_dir(&path)?;

                let submodule_name = path.file_name().unwrap().to_str().unwrap(); // Assuming file_name() is not None and is valid UTF-8
                output_path = format!("../../src/{}_bindings/", submodule_name);

                let output = Command::new("forge")
                    .arg("bind")
                    .arg("--revert-strings")
                    .arg("debug")
                    .arg("-b")
                    .arg(&output_path) // Use the dynamically generated path
                    .arg("--module")
                    .arg("--overwrite")
                    .output()?;

                if output.status.success() {
                    let output_str = String::from_utf8_lossy(&output.stdout);
                    println!("Command output: {}", output_str);
                    println!("Revert strings are on");
                } else {
                    let err_str = String::from_utf8_lossy(&output.stderr);
                    println!("Command failed, error: {}", err_str);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Command failed",
                    ));
                }
            }
        }
    }
    Ok((output_path, contracts_to_generate))
}

fn collect_contract_list(dir: &Path) -> io::Result<Vec<String>> {
    let mut contract_list = Vec::new();
    contract_list.push("shared_types".to_string());
    if dir.is_dir() {
        let dir_name = dir.file_name().unwrap().to_str().unwrap(); // Assuming file_name() is not None and is valid UTF-8

        let target_dir = if dir_name == "src" || dir_name == "contracts" {
            dir.to_path_buf()
        } else {
            // Look inside the directory for a directory named "src" or "contracts"
            let potential_src = dir.join("src");
            let potential_contracts = dir.join("contracts");
            if potential_src.is_dir() {
                potential_src
            } else if potential_contracts.is_dir() {
                potential_contracts
            } else {
                return Ok(Vec::new()); // No valid contract directory found
            }
        };

        for entry in fs::read_dir(target_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                let filename = path.file_stem().unwrap().to_str().unwrap();

                if !filename.starts_with('I') {
                    let snake_case_name = camel_to_snake_case(filename);
                    contract_list.push(snake_case_name);
                }
            }
        }
    }

    Ok(contract_list)
}

fn remove_unneeded_contracts(
    bindings_path: &Path,
    needed_contracts: Vec<String>,
) -> io::Result<()> {
    if bindings_path.is_dir() {
        for entry in fs::read_dir(bindings_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                let filename = path.file_stem().unwrap().to_str().unwrap().to_lowercase();

                // Skip if the file is `mod.rs`
                if filename == "mod" {
                    continue;
                }

                if !needed_contracts.contains(&filename) {
                    println!("Removing contract binding: {}", path.display());
                    fs::remove_file(path)?;
                }
            }
        }
    }

    update_mod_file(bindings_path, needed_contracts)?;

    Ok(())
}

fn update_mod_file(bindings_path: &Path, contracts_to_keep: Vec<String>) -> io::Result<()> {
    let mod_path = bindings_path.join("mod.rs");

    // Open the file and read its contents
    let file = File::open(&mod_path)?;
    let reader = BufReader::new(file);

    let lines: Vec<String> = reader
        .lines()
        .map_while(Result::ok)
        .filter(|line| {
            // Keep the line if it's a comment
            if line.trim().starts_with("//") || line.trim().starts_with('#') {
                return true;
            }

            // Check if the line is a module declaration and if it's one of the contracts we
            // want to keep
            if let Some(contract_name) = line
                .trim()
                .strip_prefix("pub mod ")
                .and_then(|s| s.strip_suffix(';'))
            {
                return contracts_to_keep.contains(&contract_name.to_string());
            }

            true
        })
        .collect();

    // Write the new lines back to the mod.rs
    write(&mod_path, lines.join("\n"))?;

    Ok(())
}

fn camel_to_snake_case(s: &str) -> String {
    let mut snake_case = String::new();
    let chars: Vec<char> = s.chars().collect();

    for (i, ch) in chars.iter().enumerate() {
        if ch.is_uppercase() && i != 0 {
            snake_case.push('_');
        }
        snake_case.extend(ch.to_lowercase());
    }

    snake_case
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_camel_to_snake_case() {
        assert_eq!(camel_to_snake_case("PositionRenderer"), "position_renderer");
        assert_eq!(camel_to_snake_case("Test"), "test");
        assert_eq!(
            camel_to_snake_case("AnotherTestExample"),
            "another_test_example"
        );
    }

    #[test]
    fn test_collect_contract_list_from_contracts() {
        // Create a temporary directory
        let dir = tempdir().expect("Failed to create temporary directory");

        // Create nested directories "src" and "contracts"
        let contracts_dir = dir.path().join("contracts");
        fs::create_dir(&contracts_dir).expect("Failed to create contracts directory");

        // Create files in the contracts directory
        fs::write(contracts_dir.join("ExampleContract.rs"), "").expect("Failed to write file");
        fs::write(contracts_dir.join("AnotherTest.rs"), "").expect("Failed to write file");
        fs::write(contracts_dir.join("ITestInterface.rs"), "").expect("Failed to write file"); // This should be ignored

        // Call the function
        let contracts = collect_contract_list(dir.path()).expect("Failed to collect contracts");

        // Assert the results
        let expected = vec!["shared_types", "example_contract", "another_test"];
        assert_eq!(contracts, expected);

        // Temp dir will be automatically cleaned up after going out of scope.
    }

    #[test]
    fn test_collect_contract_list_from_src() {
        // Create a temporary directory
        let dir = tempdir().expect("Failed to create temporary directory");

        // Create a nested directory "src"
        let src_dir = dir.path().join("src");
        fs::create_dir(&src_dir).expect("Failed to create src directory");

        // Create files in the src directory
        fs::write(src_dir.join("ExampleOne.rs"), "").expect("Failed to write file");
        fs::write(src_dir.join("TestTwo.rs"), "").expect("Failed to write file");

        // Call the function
        let contracts = collect_contract_list(dir.path()).expect("Failed to collect contracts");

        // Assert the results
        let expected = vec!["shared_types", "test_two", "example_one"];
        assert_eq!(contracts, expected);

        // Temp dir will be automatically cleaned up after going out of scope.
    }
    #[test]
    fn test_update_mod_file() {
        // Create a temporary directory
        let dir = tempdir().expect("Failed to create temporary directory");

        // Mock a mod.rs file with some content
        let mocked_mod_path = dir.path().join("mod.rs");
        let content = "
        // Some comments
        pub mod example_contract;
        pub mod test_contract;
        ";
        fs::write(&mocked_mod_path, content).expect("Failed to write mock mod.rs file");

        // Call the function
        let contracts_to_keep = vec!["example_contract".to_owned()];
        update_mod_file(mocked_mod_path.parent().unwrap(), contracts_to_keep)
            .expect("Failed to update mod file");

        // Open the mocked mod.rs file and check its content
        let updated_content = fs::read_to_string(&mocked_mod_path).unwrap();
        assert!(updated_content.contains("pub mod example_contract;"));
        assert!(!updated_content.contains("pub mod test_contract;"));

        // Temp dir (and the mock mod.rs file inside it) will be automatically
        // cleaned up after going out of scope.
    }
}
