use crate::errors::Error;

use std::iter::IntoIterator;
use std::path::PathBuf;

use polars::lazy::frame::{LazyCsvReader, LazyFileListReader, LazyFrame};
use polars::prelude::{lit, nth, AnyValue, QuantileInterpolOptions, SortMultipleOptions};

pub struct Analyzer {
    df: LazyFrame,
}

impl Analyzer {
    pub fn from_csv(file: &PathBuf) -> Result<Self, Error> {
        let df = LazyCsvReader::new(file)
            .with_cache(true)
            .with_has_header(true)
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
            .select(&[nth(1), nth(2)])
            .sort_by_exprs(vec![nth(1)], SortMultipleOptions::default())
            .collect()
            .map_err(|e| Error::Polars {
                context: String::from(
                            "executing the lazy selecting the 2nd and 3rd columns and sorting descending by the 2nd column",
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
            .select(&[nth(1), nth(2)])
            .sort_by_exprs(vec![nth(1)], SortMultipleOptions::default().with_order_descendings([true]))
            .collect()
            .map_err(|e| Error::Polars {
                context: String::from(
                            "executing the lazy selecting the 2nd and 3rd columns and sorting ascending by the 2nd column",
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

    pub fn percentiles(
        &self,
        percentiles: impl IntoIterator<Item = f64>,
    ) -> Result<Vec<f64>, Error> {
        let mut calculated_percentiles = Vec::new();
        let lf = self.df.clone().select(&[nth(1)]);

        for p in percentiles {
            let res = lf
                .clone()
                .quantile(lit(p), QuantileInterpolOptions::Higher)
                .collect()
                .map_err(|e| Error::Polars {
                    context: format!(
                        "executing the lazy operations for calculating the {p} percentile"
                    ),
                    source: e,
                })?;

            let v = match res.get(0) {
                Some(v) => match v[0] {
                    AnyValue::Float64(f) => f,
                    _ => panic!("unexpected type returned by quantile operation on the 1st column"),
                },
                None => {
                    return Err(Error::InvalidData {
                        reason: "CSV file is empty".into(),
                    })
                }
            };

            calculated_percentiles.push(v);
        }

        Ok(calculated_percentiles)
    }
}
