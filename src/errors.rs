use std::path::PathBuf;

/// Custom error type for RSF operations (rsf-cli specific)
#[derive(Debug)]
pub enum RsfError {
    /// File I/O error
    IoError { path: PathBuf, cause: std::io::Error },
    /// CSV parsing error
    CsvError { message: String },
    /// Schema validation error
    SchemaError { message: String },
    /// Row sorting error
    SortError,
    /// Unknown error type (includes conversions from anyhow::Error)
    Unknown(String),
}

impl RsfError {
    pub fn io_error(path: PathBuf, cause: std::io::Error) -> Self {
        RsfError::IoError { path, cause }
    }

    pub fn csv_error(message: impl Into<String>) -> Self {
        RsfError::CsvError { message: message.into() }
    }

    pub fn schema_error(message: impl Into<String>) -> Self {
        RsfError::SchemaError { message: message.into() }
    }

    pub fn sort_error() -> Self {
        RsfError::SortError
    }

    pub fn unknown(message: impl Into<String>) -> Self {
        RsfError::Unknown(message.into())
    }
}

impl std::fmt::Display for RsfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RsfError::IoError { path, cause } => write!(f, "Failed to open file '{}': {}", path.display(), cause),
            RsfError::CsvError { message } => write!(f, "CSV error: {}", message),
            RsfError::SchemaError { message } => write!(f, "Schema error: {}", message),
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

// Convert anyhow::Error to rsf-cli's RsfError (for interoperability with rsf-core)
impl From<anyhow::Error> for RsfError {
    fn from(err: anyhow::Error) -> Self {
        let msg = err.to_string();
        RsfError::unknown(msg)
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

/// Result type alias for RSF operations (rsf-cli specific)
pub type RsfResult<T> = Result<T, RsfError>;
