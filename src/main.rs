mod cli;
mod csv;
mod errors;

use errors::Error;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

fn main() -> Result<()> {
    let args = cli::Args::parse();

    match args.command {
        cli::Commands::Csv { file, analysis } => {
            csv_analysis(&file, &analysis).context("Failed to analyze CSV")
        }
    }
}

fn csv_analysis(file: &PathBuf, op: &cli::CSVAnalysis) -> Result<(), Error> {
    match op {
        cli::CSVAnalysis::Fastest => {
            let analyzer = csv::Analyzer::from_csv(file)?;
            let (secs, trace_id) = analyzer.fastest()?;
            println!("The fastest upload was: {secs} seconds (trace ID: {trace_id})");
        }
        cli::CSVAnalysis::Slowest => {
            let analyzer = csv::Analyzer::from_csv(file)?;
            let (secs, trace_id) = analyzer.slowest()?;
            println!("The slowest upload was: {secs} seconds (trace ID: {trace_id})");
        }
        cli::CSVAnalysis::Percentile { percentiles } => {
            let analyzer = csv::Analyzer::from_csv(file)?;

            let pers = percentiles.into_iter().map(|p| *p as f64 / 100.0);
            let calculated_percentiles = analyzer.percentiles(pers)?;

            println!("Percentiles");
            for (i, p) in percentiles.iter().enumerate() {
                println!("{}th: {} seconds", p, calculated_percentiles[i]);
            }
        }
    }

    Ok(())
}
