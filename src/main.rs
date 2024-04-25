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
            let secs = analyzer.fastest()?;
            println!("The fastest upload was: {} seconds", secs);
        }
        cli::CSVAnalysis::Slowest => {
            let analyzer = csv::Analyzer::from_csv(file)?;
            let secs = analyzer.slowest()?;
            println!("The slowest upload was: {} seconds", secs);
        }
        cli::CSVAnalysis::Percentile { percentiles } => {
            let analyzer = csv::Analyzer::from_csv(file)?;

            let mut output = String::from("Percentiles\n");
            for p in percentiles {
                let pf: f64 = *p as f64 / 100.0;
                let pv = analyzer.percentile(pf)?;
                output.push_str(&format!("{}th: {} seconds\n", p, pv));
            }

            print!("{output}");
        }
    }

    Ok(())
}
