/// A single table registration spec:
/// name        — the SQL name clients will use (e.g. "events")
/// root        — a file:// or s3:// URI pointing at the top-level directory
/// partitions  — zero or more folder-key names
#[derive(Debug)]
pub struct TableSpec {
    pub name: String,
    pub root: String,
    pub partitions: Vec<String>,
}

impl TableSpec {
    /// Parse strings of the form
    ///   name=path[:col1,col2,...]
    /// Examples:
    ///   events=/data/parquet/events:year,month,day
    ///   users=s3://bucket/users
    pub fn parse(s: &str) -> Result<Self, String> {
        // split off name=rest
        let (name, rest) = s
            .split_once('=')
            .ok_or_else(|| format!("Invalid table-spec `{s}`"))?;
        // split off optional :part1,part2
        let (root, parts) = if let Some((r, p)) = rest.split_once(':') {
            (r, p)
        } else {
            (rest, "")
        };
        let partitions = if parts.is_empty() {
            Vec::new()
        } else {
            parts.split(',').map(ToString::to_string).collect()
        };
        Ok(Self {
            name: name.to_string(),
            root: root.to_string(),
            partitions,
        })
    }
}
