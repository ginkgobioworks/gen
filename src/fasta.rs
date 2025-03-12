use crate::models::QueryError;
use crate::operation_management::OperationError;
use std::io::Error as IOError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FastaError {
    #[error("Operation Error: {0}")]
    OperationError(#[from] OperationError),
    #[error("IO Error: {0}")]
    IOError(#[from] IOError),
    #[error("SQL query Error: {0}")]
    SQLQueryError(#[from] QueryError),
}
