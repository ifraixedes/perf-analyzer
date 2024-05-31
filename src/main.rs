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
        cli::CSVAnalysis::All => {
            let analyzer = csv::Analyzer::from_csv(file)?;
            let (fastest, fastest_trace_id) = analyzer.fastest()?;
            let (slowest, slowest_trace_id) = analyzer.slowest()?;
            let percentiles = analyzer.percentiles([0.5, 0.99])?;

            println!(
                r#"Operations results
Fastest: {fastest} seconds (trace ID: {fastest_trace_id})
Slowest: {slowest} seconds (trace ID: {slowest_trace_id})
Percentiles:
  - 50th: {} seconds
  - 99th: {} seconds
"#,
                percentiles[0], percentiles[1]
            )
        }
        cli::CSVAnalysis::Fastest => {
            let analyzer = csv::Analyzer::from_csv(file)?;
            let (secs, trace_id) = analyzer.fastest()?;
            println!("The fastest operation: {secs} seconds (trace ID: {trace_id})");
        }
        cli::CSVAnalysis::Slowest => {
            let analyzer = csv::Analyzer::from_csv(file)?;
            let (secs, trace_id) = analyzer.slowest()?;
            println!("The slowest operation: {secs} seconds (trace ID: {trace_id})");
        }
        cli::CSVAnalysis::Percentile { percentiles } => {
            let analyzer = csv::Analyzer::from_csv(file)?;

            let pers = percentiles.into_iter().map(|p| *p as f64 / 100.0);
            let calculated_percentiles = analyzer.percentiles(pers)?;

            if percentiles.len() == 1 {
                println!(
                    "{}th percentile: {} seconds",
                    percentiles[0], calculated_percentiles[0]
                );
            } else {
                println!("Percentiles");
                for (i, p) in percentiles.iter().enumerate() {
                    println!("  - {}th: {} seconds", p, calculated_percentiles[i]);
                }
            }
        }
    }

    Ok(())
}
