use super::*;

/// The token requester is responsible for requesting tokens from the token
/// admin. This agents is purely for testing purposes as far as I can tell.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct TokenRequester {
    /// The tokens that the token requester has requested.
    pub token_data: TokenData,
    /// The agent ID to request tokens to.
    pub request_to: String,
    /// Client to have an address to receive token mint to and check balance
    #[serde(skip)]
    pub client: Option<Arc<ArbiterMiddleware>>,
    /// The messaging layer for the token requester.
    #[serde(skip)]
    pub messager: Option<Messager>,
    #[serde(default)]
    pub count: u64,
    #[serde(default = "default_max_count")]
    pub max_count: Option<u64>,
}

impl TokenRequester {
    pub fn new(max_count: Option<u64>) -> Self {
        Self {
            token_data: TokenData {
                name: TOKEN_NAME.to_owned(),
                symbol: TOKEN_SYMBOL.to_owned(),
                decimals: TOKEN_DECIMALS,
                address: None,
            },
            request_to: TOKEN_ADMIN_ID.to_owned(),
            client: None,
            messager: None,
            count: 0,
            max_count,
        }
    }
}
