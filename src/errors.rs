use anyhow::Error;
use std::path::PathBuf;

/// Custom error type for RSF operations
#[derive(Debug)]
pub enum RsfError {
    /// File I/O error
    IoError {
        path: PathBuf,
        cause: std::io::Error,
    },
    /// CSV parsing error
    CsvError { message: String },
    /// Schema validation error
    SchemaError { message: String },
    /// Invalid column ordering
    ColumnOrderError {
        position: usize,
        expected: String,
        found: String,
    },
    /// Invalid cardinality ranking
    CardinalityError {
        column: String,
        expected: usize,
        found: usize,
    },
    /// Row sorting error
    SortError,
    /// Unknown error type
    Unknown(String),
}

impl RsfError {
    /// Create an I/O error with context
    pub fn io_error(path: PathBuf, cause: std::io::Error) -> Self {
        RsfError::IoError { path, cause }
    }

    /// Create a CSV parsing error
    pub fn csv_error(message: impl Into<String>) -> Self {
        RsfError::CsvError {
            message: message.into(),
        }
    }

    /// Create a schema validation error
    pub fn schema_error(message: impl Into<String>) -> Self {
        RsfError::SchemaError {
            message: message.into(),
        }
    }

    /// Create a column order error
    pub fn column_order_error(position: usize, expected: String, found: String) -> Self {
        RsfError::ColumnOrderError {
            position,
            expected,
            found,
        }
    }

    /// Create a cardinality error
    pub fn cardinality_error(column: String, expected: usize, found: usize) -> Self {
        RsfError::CardinalityError {
            column,
            expected,
            found,
        }
    }

    /// Create a sort error
    pub fn sort_error() -> Self {
        RsfError::SortError
    }

    /// Create an unknown error
    pub fn unknown(message: impl Into<String>) -> Self {
        RsfError::Unknown(message.into())
    }
}

impl std::fmt::Display for RsfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RsfError::IoError { path, cause } => {
                write!(f, "Failed to open file '{}': {}", path.display(), cause)
            }
            RsfError::CsvError { message } => write!(f, "CSV error: {}", message),
            RsfError::SchemaError { message } => write!(f, "Schema error: {}", message),
            RsfError::ColumnOrderError {
                position,
                expected,
                found,
            } => {
                write!(
                    f,
                    "Column order mismatch at position {}: expected '{}', found '{}'",
                    position, expected, found
                )
            }
            RsfError::CardinalityError {
                column,
                expected,
                found,
            } => {
                write!(
                    f,
                    "Column '{}' has invalid cardinality: expected {}, found {}",
                    column, expected, found
                )
            }
            RsfError::SortError => write!(f, "Rows are not in canonical sorted order"),
            RsfError::Unknown(message) => write!(f, "Unknown error: {}", message),
        }
    }
}

impl std::error::Error for RsfError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RsfError::IoError { cause, .. } => Some(cause),
            _ => None,
        }
    }
}

impl From<std::io::Error> for RsfError {
    fn from(err: std::io::Error) -> Self {
        RsfError::io_error(PathBuf::from("<unknown>"), err)
    }
}

impl From<csv::Error> for RsfError {
    fn from(err: csv::Error) -> Self {
        RsfError::csv_error(err.to_string())
    }
}

impl From<serde_yaml::Error> for RsfError {
    fn from(err: serde_yaml::Error) -> Self {
        RsfError::schema_error(err.to_string())
    }
}

/// Convert RsfError to anyhow::Error with context
pub trait IntoAnyhow {
    fn into_anyhow(self) -> Error;
}

impl IntoAnyhow for RsfError {
    fn into_anyhow(self) -> Error {
        Error::new(self).context("RSF operation failed")
    }
}

/// Result type alias for RSF operations
pub type RsfResult<T> = Result<T, RsfError>;
