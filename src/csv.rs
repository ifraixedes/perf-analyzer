use crate::errors::Error;

use std::path::PathBuf;
use std::sync::Arc;

use polars::datatypes::DataType;
use polars::lazy::frame::{LazyCsvReader, LazyFileListReader, LazyFrame};
use polars::prelude::{col, AnyValue, Expr, LiteralValue, QuantileInterpolOptions, Schema};

const COLUMNS: &[&str] = &["timestamp", "elapsedtime_seconds", "trace_id", "jaeger_url"];

pub struct Analyzer {
    df: LazyFrame,
}

impl Analyzer {
    pub fn from_csv(file: &PathBuf) -> Result<Self, Error> {
        let mut schema = Schema::with_capacity(3);
        schema.with_column(COLUMNS[0].into(), DataType::String);
        schema.with_column(COLUMNS[1].into(), DataType::Float64);
        schema.with_column(COLUMNS[2].into(), DataType::String);
        schema.with_column(COLUMNS[3].into(), DataType::String);

        let df = LazyCsvReader::new(file)
            .has_header(true)
            .with_schema(Some(Arc::new(schema)))
            .with_cache(true)
            .finish_no_glob()
            .map_err(|e| Error::Polars {
                context: format!(
                    "creating LazyFrame from CSV file {}",
                    file.to_str().unwrap_or("")
                ),
                source: e,
            })?;

        Ok(Self { df })
    }

    // TODO: provide all the data of the row.
    pub fn fastest(&self) -> Result<f64, Error> {
        self.df
            .clone()
            .select(&[col(COLUMNS[1])])
            .min()
            .map_err(|e| Error::Polars {
                context: format!(r#"creating query for calculating the minimum of "{}" column"#, COLUMNS[1]),
                source: e,
            })?
            .collect()
            .map_err(|e| Error::Polars {
                context: format!(r#"executing the lazy operations for calculating the minimum value of the "{} column"#, COLUMNS[1]),
                source: e,
            })?
            .column(COLUMNS[1])
            .map_err(|e| Error::Polars {
                context: format!(r#"selecting column "{}" from collected data frame""#, COLUMNS[1]),
                source: e,
            })?
            .min::<f64>()
            .map_err(|e| Error::Polars {
                context: format!(r#"calculating the minimum value of the column "{}" from collected data frame""#, COLUMNS[1]),
                source: e,
            })?
            .ok_or_else(|| Error::InvalidData { reason: "CSV file is empty".into() })
    }

    // TODO: provide all the data of the row.
    pub fn slowest(&self) -> Result<f64, Error> {
        self.df
            .clone()
            .select(&[col(COLUMNS[1])])
            .max()
            .map_err(|e| Error::Polars {
                context: format!(r#"creating query for calculating the maximum of "{}" column"#, COLUMNS[1]),
                source: e,
            })?
            .collect()
            .map_err(|e| Error::Polars {
                context: format!(r#"executing the lazy operations for calculating the maximum value of the "{} column"#, COLUMNS[1]),
                source: e,
            })?
            .column(COLUMNS[1])
            .map_err(|e| Error::Polars {
                context: format!(r#"selecting column "{}" from collected data frame""#, COLUMNS[1]),
                source: e,
            })?
            .max::<f64>()
            .map_err(|e| Error::Polars {
                context: format!(r#"calculating the maximum value of the column "{}" from collected data frame""#, COLUMNS[1]),
                source: e,
            })?
            .ok_or_else(|| Error::InvalidData { reason: "CSV file is empty".into() })
    }

    pub fn percentile(&self, percentile: f64) -> Result<f64, Error> {
        let res = self
            .df
            .clone()
            .select(&[col(COLUMNS[1])])
            .quantile(
                Expr::Literal(LiteralValue::Float64(percentile)),
                QuantileInterpolOptions::Higher,
            )
            .map_err(|e| Error::Polars {
                context: format!("creating query for calculating the {percentile} percentile"),
                source: e,
            })?
            .collect()
            .map_err(|e| Error::Polars {
                context: format!(
                    "executing the lazy operations for calculating the {percentile} percentile"
                ),
                source: e,
            })?;

        match res.get(0) {
            Some(v) => match v[0] {
                AnyValue::Float64(f) => Ok(f),
                _ => panic!(
                    r#"unexpected type returned by quantile operation on "{}" column"#,
                    COLUMNS[1],
                ),
            },
            None => Err(Error::InvalidData {
                reason: "CSV file is empty".into(),
            }),
        }
    }
}
