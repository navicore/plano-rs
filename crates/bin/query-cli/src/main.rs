use clap::Parser;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::prelude::*;
use glob::glob;
use rustyline::config::EditMode;
use rustyline::history::MemHistory;
use rustyline::{error::ReadlineError, Config, Editor};

/// Run SQL queries against one or more Parquet files using DataFusion
#[derive(Parser, Debug)]
#[command(name = "query-cli")]
struct Args {
    /// One or more --table name=glob_pattern entries
    #[arg(short, long, required = true, value_parser = parse_table)]
    table: Vec<(String, String)>,

    /// SQL query to run
    #[arg(short, long)]
    query: Option<String>,

    /// Start an interactive REPL
    #[arg(long)]
    repl: bool,
}

fn parse_table(s: &str) -> Result<(String, String), String> {
    let parts: Vec<_> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err("Expected format: name=glob".to_string());
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let args = Args::parse();
    let ctx = SessionContext::new();

    for (table_name, pattern) in &args.table {
        let file_paths: Vec<_> = glob(pattern)
            .expect("Invalid glob pattern")
            .filter_map(Result::ok)
            .filter(|path| {
                path.extension()
                    .map(|ext| ext == "parquet")
                    .unwrap_or(false)
            })
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        if file_paths.is_empty() {
            eprintln!(
                "No parquet files matched pattern for table '{}': {}",
                table_name, pattern
            );
            std::process::exit(1);
        }

        let df = ctx
            .read_parquet(file_paths.clone(), ParquetReadOptions::default())
            .await?;
        ctx.register_table(table_name, df.into_view())?;
    }

    if args.repl {
        let config = Config::builder().edit_mode(EditMode::Vi).build();
        let mut rl = Editor::<(), MemHistory>::with_history(config, MemHistory::new())
            .expect("Failed to initialize rustyline with history");
        rl.set_helper(None);

        loop {
            match rl.readline("query> ") {
                Ok(input) => {
                    let input = input.trim();
                    if input == ".exit" {
                        break;
                    } else if input == ".tables" {
                        if let Some(schema) =
                            ctx.catalog("datafusion").and_then(|c| c.schema("public"))
                        {
                            for table in schema.table_names() {
                                println!("{}", table);
                            }
                        } else {
                            eprintln!("[.tables] failed to access default schema.");
                        }

                        continue;
                    }
                    if !input.is_empty() {
                        let _ = rl.add_history_entry(input);
                        match ctx.sql(input).await {
                            Ok(df) => match df.collect().await {
                                Ok(results) => {
                                    if let Err(e) = print_batches(&results) {
                                        eprintln!("Error printing results: {e}");
                                    }
                                }
                                Err(e) => eprintln!("Execution error: {e}"),
                            },
                            Err(e) => eprintln!("Query error: {e}"),
                        }
                    }
                }
                Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
                Err(err) => {
                    eprintln!("Readline error: {:?}", err);
                    break;
                }
            }
        }
    } else if let Some(query) = args.query {
        let df = ctx.sql(&query).await?;
        let results = df.collect().await?;
        print_batches(&results)?;
    } else {
        eprintln!("Either --query or --repl must be provided.");
    }

    Ok(())
}
