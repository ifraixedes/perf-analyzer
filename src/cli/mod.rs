use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Commands for CSV files which contains the elapsed time of an operation in seconds.
    ///
    /// Each row is the execution of the same operation.
    /// The CSV file is expected to have 4 columns: timestamp, elapsed time (seconds), trace id, jaeger_url
    Csv {
        /// CSV file to process
        #[arg(value_name = "FILE")]
        file: PathBuf,
        #[command(subcommand)]
        analysis: CSVAnalysis,
    },
}

#[derive(Subcommand)]
pub enum CSVAnalysis {
    /// Calculates all the operations using defaults values
    All,
    /// Calculates the fastest operation
    Fastest,
    /// Calculates the slowest operation
    Slowest,
    /// Calculate the percentile from the CSV file
    Percentile {
        #[arg(default_values_t = [50, 99])]
        percentiles: Vec<u8>,
    },
}
