use crate::errors::Error;

use std::path::PathBuf;
use std::sync::Arc;

use polars::datatypes::DataType;
use polars::prelude::{
    col, AnyValue, CsvReader, DataFrame, Expr, IntoLazy, LiteralValue, QuantileInterpolOptions,
    Schema, SerReader,
};

pub struct Analyzer {
    df: DataFrame,
}

impl Analyzer {
    pub fn from_csv(file: &PathBuf) -> Result<Self, Error> {
        let mut schema = Schema::with_capacity(3);
        schema.with_column("timestamp".into(), DataType::String);
        schema.with_column("elapsedtime_seconds".into(), DataType::Float64);
        schema.with_column("trace_id".into(), DataType::String);
        schema.with_column("jaeger_url".into(), DataType::String);

        // TODO: change it to use LazyCsvReader
        let df = CsvReader::from_path(file)
            .map_err(|e| Error::Polars {
                context: format!("reading CSV file '{}'", file.to_str().unwrap_or("")),
                source: e,
            })?
            .has_header(true)
            .with_schema(Some(Arc::new(schema)))
            .finish()
            .map_err(|e| Error::Polars {
                context: format!(
                    "creating a DataFrame from CSV file {}",
                    file.to_str().unwrap_or("")
                ),
                source: e,
            })?;

        if df.is_empty() {
            return Err(Error::InvalidData {
                reason: format!(
                    "there is no data in the CSV file {}",
                    file.to_str().unwrap_or(""),
                ),
            });
        }

        Ok(Self { df })
    }

    // TODO: provide all the data of the row.
    pub fn fastest(&self) -> Result<f64, Error> {
        Ok(self.df
            .column("elapsedtime_seconds")
            .map_err(|e| Error::Polars { context: r#"selecting column "elapsedtime_seconds""#.into(), source: e })?
            .min::<f64>()
            .map_err(|e| Error::Polars { context:r#"calculating the minimum value of the "elapsedtime_seconds" column"#.into(), source: e })?
            .expect("BUG: missing a checks on DataFrame creation or modification to ensure that they are not empty")
        )
    }

    // TODO: provide all the data of the row.
    pub fn slowest(&self) -> Result<f64, Error> {
        Ok(self.df
            .column("elapsedtime_seconds")
            .map_err(|e| Error::Polars { context: r#"selecting column "elapsedtime_seconds""#.into(), source: e })?
            .max::<f64>()
            .map_err(|e| Error::Polars { context:r#"calculating the minimum value of the "elapsedtime_seconds" column"#.into(), source: e })?
            .expect("BUG: missing a checks on DataFrame creation or modification to ensure that they are not empty")
        )
    }

    pub fn percentile(&self, percentile: f64) -> Result<f64, Error> {
        let res = self
            .df
            .clone()
            .lazy()
            .select(&[col("elapsedtime_seconds")])
            .quantile(
                Expr::Literal(LiteralValue::Float64(percentile)),
                QuantileInterpolOptions::Higher,
            )
            .map_err(|e| Error::Polars {
                context: format!("applying quantile operation for percentile: {percentile}"),
                source: e,
            })?
            .collect()
            .map_err(|e| Error::Polars {
                context: format!("executing quantile operation for percentile: {percentile}"),
                source: e,
            })?;

        match res.get(0) {
            Some(v) => match v[0] {
                AnyValue::Float64(f) => Ok(f),
                _ => panic!(
                    r#"unexpected type returned by quantile operation on "elapsedtime_seconds" column"#
                ),
            },
            None => panic!(
                r#"BUG: missing a checks on DataFrame creation or modification to ensure that they are not empty"#
            ),
        }
    }
}
