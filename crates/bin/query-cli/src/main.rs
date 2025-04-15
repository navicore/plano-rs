use clap::Parser;
use datafusion::prelude::*;
use glob::glob;
use plano_core::format::{format_batches, OutputFormat};
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::Config;
use rustyline::Editor as LineEditor;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "query-cli")]
struct Args {
    /// Start in interactive REPL mode
    #[arg(long)]
    repl: bool,
    /// One or more --table name=glob_pattern entries
    #[arg(short, long, required = true, value_parser = parse_table)]
    table: Vec<(String, String)>,

    /// Optional SQL query to run directly
    #[arg(long)]
    query: Option<String>,

    /// Optional output format: text, csv, or json
    #[arg(long, default_value = "text")]
    format: String,
}

fn parse_table(s: &str) -> Result<(String, String), String> {
    let parts: Vec<_> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err("Expected format: name=glob".to_string());
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let ctx = SessionContext::new();
    let mut table_paths: HashMap<String, Vec<String>> = HashMap::new();

    for (name, pattern) in &args.table {
        let files: Vec<_> = glob(pattern)
            .expect("Invalid glob pattern")
            .filter_map(Result::ok)
            .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        if files.is_empty() {
            eprintln!("No files matched for table '{}': {}", name, pattern);
            continue;
        }

        table_paths.insert(name.clone(), files);
    }

    for (name, files) in table_paths.iter() {
        let df = ctx
            .read_parquet(files.clone(), ParquetReadOptions::default())
            .await?;
        ctx.register_table(name, df.into_view())?;
    }

    let format = match args.format.as_str() {
        "json" => OutputFormat::Json,
        "csv" => OutputFormat::Csv,
        _ => OutputFormat::Text,
    };

    if let Some(sql) = args.query {
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        let output = format_batches(&batches, format).map_err(|e| anyhow::anyhow!(e))?;
        println!("{}", output);
        return Ok(());
    }

    if !args.repl {
        eprintln!("No query provided and --repl not set. Exiting.");
        return Ok(());
    }

    // REPL mode
    let config = Config::builder().auto_add_history(true).build();
    let history_path = dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("query-cli-history.txt");

    let mut rl = LineEditor::<(), FileHistory>::with_history(config, FileHistory::new())?;
    rl.load_history(&history_path).ok();

    loop {
        let line = rl.readline("query> ");
        match line {
            Ok(line) => {
                let sql = line.trim();
                if sql.eq_ignore_ascii_case(".exit") {
                    break;
                }
                if sql.eq_ignore_ascii_case(".tables") {
                    if let Some(schema) = ctx.catalog("datafusion").and_then(|c| c.schema("public"))
                    {
                        for t in schema.table_names() {
                            println!("{}", t);
                        }
                    }
                    continue;
                }
                match ctx.sql(sql).await {
                    Ok(df) => match df.collect().await {
                        Ok(batches) => {
                            match format_batches(&batches, format.clone())
                                .map_err(|e| anyhow::anyhow!(e))
                            {
                                Ok(output) => println!("{}", output),
                                Err(e) => eprintln!("format error: {e}"),
                            }
                        }
                        Err(e) => eprintln!("query error: {e}"),
                    },
                    Err(e) => eprintln!("sql error: {e}"),
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(err) => {
                eprintln!("Error: {err}");
                break;
            }
        }
    }

    rl.save_history(&history_path).ok();
    Ok(())
}
