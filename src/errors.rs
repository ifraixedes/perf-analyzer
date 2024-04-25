use polars::error::PolarsError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid data: {reason}")]
    InvalidData { reason: String },
    #[error("{context}")]
    Polars {
        context: String,
        source: PolarsError,
    },
}
