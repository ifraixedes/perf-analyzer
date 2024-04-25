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
    /// Commands for CSV files
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
    Fastest,
    Slowest,
    /// Calculate the percentile from the CSV file
    Percentile {
        percentiles: Vec<u8>,
    },
}
