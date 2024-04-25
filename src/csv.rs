use crate::errors::Error;

use std::path::PathBuf;
use std::sync::Arc;

use polars::datatypes::DataType;
use polars::lazy::frame::{LazyCsvReader, LazyFileListReader, LazyFrame};
use polars::prelude::{col, lit, AnyValue, QuantileInterpolOptions, Schema, SortMultipleOptions};

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

    pub fn fastest(&self) -> Result<(f64, String), Error> {
        let df = self.df
            .clone()
            .select(&[col(COLUMNS[1]), col(COLUMNS[2]) ])
            .sort([COLUMNS[1]], SortMultipleOptions::default().with_order_descendings([true, false]))
            .collect()
            .map_err(|e| Error::Polars {
                context: format!(
                             r#"executing the lazy selecting the "{}" and "{}""columns and sorting descending by the "{0}" column"#,
                             COLUMNS[1],
                             COLUMNS[2],
                             ),
                source: e,
            })?;

        let row = df.get(0).ok_or_else(|| Error::InvalidData {
            reason: "CSV file is empty".into(),
        })?;

        let (elapsed_time, trace_id) = match &row[..] {
            [e, t] => (e, t),
            _ => panic!("BUG: the row should have 2 columns"),
        };

        let elapsed_time = if let AnyValue::Float64(e) = elapsed_time {
            e
        } else {
            panic!("BUG: the first column should be a float64");
        };

        let trace_id = if let AnyValue::String(t) = trace_id {
            t
        } else {
            panic!("BUG: the second column should be a String");
        };

        Ok((*elapsed_time, (*trace_id).into()))
    }

    pub fn slowest(&self) -> Result<(f64, String), Error> {
        let df = self.df
            .clone()
            .select(&[col(COLUMNS[1]), col(COLUMNS[2]) ])
            .sort([COLUMNS[1]], SortMultipleOptions::default().with_order_descendings([false, false]))
            .collect()
            .map_err(|e| Error::Polars {
                context: format!(
                             r#"executing the lazy selecting the "{}" and "{}""columns and sorting ascending by the "{0}" column"#,
                             COLUMNS[1],
                             COLUMNS[2],
                             ),
                source: e,
            })?;

        let row = df.get(0).ok_or_else(|| Error::InvalidData {
            reason: "CSV file is empty".into(),
        })?;

        let (elapsed_time, trace_id) = match &row[..] {
            [e, t] => (e, t),
            _ => panic!("BUG: the row should have 2 columns"),
        };

        let elapsed_time = if let AnyValue::Float64(e) = elapsed_time {
            e
        } else {
            panic!("BUG: the first column should be a float64");
        };

        let trace_id = if let AnyValue::String(t) = trace_id {
            t
        } else {
            panic!("BUG: the second column should be a String");
        };

        Ok((*elapsed_time, (*trace_id).into()))
    }

    pub fn percentile(&self, percentile: f64) -> Result<f64, Error> {
        let res = self
            .df
            .clone()
            .select(&[col(COLUMNS[1])])
            .quantile(lit(percentile), QuantileInterpolOptions::Higher)
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
