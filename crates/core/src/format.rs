/// Format module for handling output of record batches in different formats.
use datafusion::arrow::{
    array::RecordBatch, csv::writer::WriterBuilder, json::writer::LineDelimitedWriter,
    util::pretty::pretty_format_batches,
};
use std::io::Cursor;

/// Enum representing the output format for record batches.
#[derive(Debug, Clone, Eq, PartialEq)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::{
            array::{Int32Array, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        },
        common::assert_contains,
    };
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));
        RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap()
    }

    #[test]
    fn test_format_batches_json() {
        let batch = create_test_batch();
        let result = format_batches(&[batch], OutputFormat::Json).unwrap();
        assert!(result.contains(r#"{"id":1,"name":"Alice"}"#));
    }

    #[test]
    fn test_format_batches_csv() {
        let batch = create_test_batch();
        let result = format_batches(&[batch], OutputFormat::Csv).unwrap();
        assert!(result.contains("id,name\n1,Alice\n2,Bob\n3,Charlie"));
    }

    #[test]
    fn test_format_batches_text() {
        let batch = create_test_batch();
        let result = format_batches(&[batch], OutputFormat::Text).unwrap();
        assert_contains!(&result, "+----+---------+");
        assert_contains!(&result, "| id | name    |");
    }
}
