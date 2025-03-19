use anyhow::Result;
use hostname;
use std::env;

pub fn my_name() -> Result<String> {
    Ok(hostname::get()?.to_string_lossy().to_string())
}

pub fn slurm_nodelist() -> Result<Vec<String>> {
    parse_slurm_nodelist(&env::var("SLURM_NODELIST")?)
}

/// Parses SLURM nodelist string into a vector of node names
/// Handles various SLURM node list formats like:
/// - node[1-3]
/// - node1,node2,node3
/// - node[1,3-5]
fn parse_slurm_nodelist(nodelist: &str) -> Result<Vec<String>> {
    let mut nodes = Vec::new();
    let nodelist = nodelist.trim();

    for part in nodelist.split(',') {
        if part.contains('[') {
            let (base, rest) = part.split_once('[').unwrap();
            let rest = rest.trim_end_matches(']');

            if rest.contains('-') {
                let (start, end) = rest.split_once('-').unwrap();
                let start: i32 = start.parse()?;
                let end: i32 = end.parse()?;

                for i in start..=end {
                    nodes.push(format!("{}{}", base, i));
                }
            } else {
                let indices: Vec<i32> = rest.split(',').map(|x| x.parse().unwrap()).collect();

                for i in indices {
                    nodes.push(format!("{}{}", base, i));
                }
            }
        } else {
            nodes.push(part.to_string());
        }
    }

    let name = my_name().unwrap();
    nodes.retain(|x| *x != name);
    Ok(nodes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_slurm_nodelist() {
        let test_cases = vec![
            ("node[1-3]", vec!["node1", "node2", "node3"]),
            ("node1,node2,node3", vec!["node1", "node2", "node3"]),
            ("node[1,3-5]", vec!["node1", "node3", "node4", "node5"]),
        ];

        for (input, expected) in test_cases {
            assert_eq!(parse_slurm_nodelist(input), expected);
        }
    }
}
