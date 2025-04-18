/// Format module for handling output of record batches in different formats.
use datafusion::arrow::{
    array::RecordBatch, csv::writer::WriterBuilder, json::writer::LineDelimitedWriter,
    util::pretty::pretty_format_batches,
};
use std::io::Cursor;

/// Enum representing the output format for record batches.
#[derive(Debug, Clone)]
pub enum OutputFormat {
    Json,
    Csv,
    Text,
}

/// Formats the given record batches into a string representation based on the specified output format.
/// ## Errors
pub fn format_batches(batches: &[RecordBatch], format: OutputFormat) -> Result<String, String> {
    match format {
        OutputFormat::Json => {
            let mut buffer = Cursor::new(Vec::new());
            {
                let mut writer = LineDelimitedWriter::new(&mut buffer);
                for batch in batches {
                    writer.write(batch).map_err(|e| e.to_string())?;
                }
                writer.finish().map_err(|e| e.to_string())?;
            }
            String::from_utf8(buffer.into_inner()).map_err(|e| e.to_string())
        }
        OutputFormat::Csv => {
            let mut buffer = Cursor::new(Vec::new());
            {
                let mut writer = WriterBuilder::new().build(&mut buffer);
                for batch in batches {
                    writer.write(batch).map_err(|e| e.to_string())?;
                }
            }
            String::from_utf8(buffer.into_inner()).map_err(|e| e.to_string())
        }
        OutputFormat::Text => pretty_format_batches(batches)
            .map(|d| d.to_string())
            .map_err(|e| e.to_string()),
    }
}
