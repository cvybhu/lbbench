use anyhow::Result;
use clap::{ArgEnum, Parser};
use scylla::{Session, SessionBuilder};
use std::collections::HashMap;
use std::io::Write;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(arg_enum, value_parser, default_value = "simple")]
    mode: Mode,

    #[clap(short, long, value_parser, default_value = "127.0.0.1:9042")]
    node: String,

    #[clap(short, long, value_parser)]
    verbose: bool,

    #[clap(short, long, value_parser, default_value_t = 1000_000usize)]
    max_replication_factor: usize,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
enum Mode {
    Simple,
    Bigsimple,
    Nts,
    Bignts,
    Combined,
}

#[derive(Debug)]
struct ClusterInfo {
    total_nodes: usize,
    datacenter_sizes: HashMap<String, usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let session: Session = SessionBuilder::new().known_node(args.node).build().await?;

    clear_existing_keyspaces(&session, args.verbose).await?;
    let cluster_info: ClusterInfo = get_cluster_info(&session, args.verbose).await?;

    generate_keyspaces(
        &session,
        &cluster_info,
        args.mode,
        args.verbose,
        args.max_replication_factor,
    )
    .await?;

    create_tables(&session, args.verbose).await?;

    println!("Done!");

    Ok(())
}

async fn fetch_non_system_keyspaces(session: &Session, verbose: bool) -> Result<Vec<String>> {
    let keyspaces: Vec<String> = session
        .query("SELECT keyspace_name FROM system_schema.keyspaces", ())
        .await?
        .rows_typed::<(String,)>()?
        .map(|res| res.map(|row| row.0))
        .map(|res| res.map_err(|r| r.into()))
        .collect::<Result<Vec<String>>>()?;

    if verbose {
        println!("Found keyspaces: {:?}", keyspaces);
    }

    let builtin_keyspaces = [
        "system",
        "system_auth",
        "system_schema",
        "system_distributed",
        "system_traces",
        "system_distributed_everywhere",
    ];

    let mut result: Vec<String> = keyspaces
        .into_iter()
        .filter(|ks_name| !builtin_keyspaces.contains(&ks_name.as_str()))
        .collect();

    result.sort();

    Ok(result)
}

async fn clear_existing_keyspaces(session: &Session, verbose: bool) -> Result<()> {
    session.refresh_metadata().await?;

    let keyspaces: Vec<String> = fetch_non_system_keyspaces(session, verbose).await?;
    println!("Found {} keyspaces to clean", keyspaces.len());

    for (i, keyspace_name) in keyspaces.iter().enumerate() {
        let query = format!("DROP KEYSPACE {keyspace_name}");

        if verbose {
            println!("Cleaning {}/{}: {}", i, keyspaces.len(), query);
        } else {
            print!("{i} ");
        }
        std::io::stdout().flush()?;

        session.query(query, ()).await?;
    }

    println!("\nKeyspaces cleaned!");

    Ok(())
}

async fn get_cluster_info(session: &Session, verbose: bool) -> Result<ClusterInfo> {
    session.refresh_metadata().await?;

    let cluster_data = session.get_cluster_data();

    let total_nodes: usize = cluster_data.get_nodes_info().len();
    let mut datacenter_sizes: HashMap<String, usize> = HashMap::new();

    for node in cluster_data.get_nodes_info() {
        if let Some(dc) = &node.datacenter {
            *datacenter_sizes.entry(dc.clone()).or_insert(0) += 1;
        }
    }

    let cluster_info = ClusterInfo {
        total_nodes,
        datacenter_sizes,
    };

    if verbose {
        println!("Got ClusterInfo:\n{:#?}", cluster_info);
    }

    Ok(cluster_info)
}

fn random_int(inputs: impl AsRef<[usize]>, upper: usize) -> usize {
    let bigprime: u128 = 1000000007;
    let mut result: u128 = inputs.as_ref().len() as u128;
    for i in inputs.as_ref() {
        let mut x: u128 = (i + 10) as u128;
        for _ in 0..16 {
            x = (x * x) % bigprime;
            result += x;
            result %= bigprime;
            result *= result;
            result %= bigprime;
        }

        for _ in 0..16 {
            result *= result;
            result %= bigprime;
        }
    }

    (result % (upper as u128)) as usize
}

async fn generate_keyspaces(
    session: &Session,
    cluster_info: &ClusterInfo,
    mode: Mode,
    verbose: bool,
    max_replication_factor: usize,
) -> Result<()> {
    if mode == Mode::Simple || mode == Mode::Bigsimple || mode == Mode::Combined {
        let simple_num = 20;
        println!("Creating {simple_num} SimpleStrategy keyspaces with RF = 1..=8");

        for ks_id in 0..simple_num {
            let mut repfactor = 1 + random_int([ks_id, 3434], 8);
            if repfactor > cluster_info.total_nodes {
                repfactor = cluster_info.total_nodes;
            }
            if repfactor > max_replication_factor {
                repfactor = max_replication_factor;
            }
            let query = format!("CREATE KEYSPACE simple{ks_id} with replication = {{'class': 'SimpleStrategy', 'replication_factor': {repfactor}}}");
            if verbose {
                println!("{ks_id}/{simple_num}: {query}");
            } else {
                print!("{ks_id} ");
            }
            std::io::stdout().flush()?;
            session.query(query, ()).await?;
        }

        if !verbose {
            println!();
        }
        println!("Created");
    }

    if mode == Mode::Bigsimple || mode == Mode::Combined {
        let bigsimple_num = 8;
        println!(
            "Creating {} SimpleStrategy keyspaces with RF~{}",
            bigsimple_num, cluster_info.total_nodes
        );

        for ks_id in 0..bigsimple_num {
            let to_substract = random_int([ks_id, 1834], 8);
            let mut repfactor = cluster_info
                .total_nodes
                .checked_sub(to_substract)
                .unwrap_or(1);
            if repfactor == 0 {
                repfactor = 1;
            }
            if repfactor > max_replication_factor {
                repfactor = max_replication_factor;
            }

            let query = format!("CREATE KEYSPACE bigsimple{ks_id} with replication = {{'class': 'SimpleStrategy', 'replication_factor': {repfactor}}}");

            if verbose {
                println!("{ks_id}/{bigsimple_num}: {query}");
            } else {
                print!("{ks_id} ");
            }
            std::io::stdout().flush()?;

            session.query(query, ()).await?;
        }

        if !verbose {
            println!();
        }
        println!("Created");
    }

    if mode == Mode::Nts || mode == Mode::Combined {
        let nts_num = 32;

        println!("Creating {nts_num} NetworkTopologyStrategy keyspaces with RF=1..=8");

        for ks_id in 0..nts_num {
            let mut dc_repfactors: Vec<(&str, usize)> = Vec::new();

            for (i, (dc_name, dc_size)) in cluster_info.datacenter_sizes.iter().enumerate() {
                let mut cur_repfactor: usize = 1 + random_int([ks_id, i], 8);

                if cur_repfactor > *dc_size {
                    cur_repfactor = *dc_size;
                }
                if cur_repfactor > max_replication_factor {
                    cur_repfactor = max_replication_factor;
                }

                dc_repfactors.push((dc_name, cur_repfactor));
            }

            // Sometimes drop 1 dc from the NTS, it doesn't have to contain all DCs.
            if dc_repfactors.len() > 1 && ks_id % 5 == 0 {
                let to_drop: usize = (ks_id * ks_id + 2323) % dc_repfactors.len();
                dc_repfactors.remove(to_drop);
            }

            let mut dc_repfactors_str: String = String::new();
            for (dc_name, dc_repfactor) in dc_repfactors {
                dc_repfactors_str += &format!(", '{dc_name}': {dc_repfactor}");
            }

            let query_str = format!("CREATE KEYSPACE nts{ks_id} with replication = {{'class': 'NetworkTopologyStrategy'{dc_repfactors_str}}}");
            if verbose {
                println!("{ks_id}/{nts_num}: {query_str}");
            } else {
                print!("{ks_id} ");
            }
            std::io::stdout().flush()?;
            session.query(query_str, ()).await?;
        }

        if !verbose {
            println!();
        }
        println!("Created");
    }

    if mode == Mode::Bignts || mode == Mode::Combined {
        let bignts_num = 8;
        println!("Creating {bignts_num} NetworkTopologyStrategy keyspaces with RF~dcsize");

        for ks_id in 0..bignts_num {
            let mut dc_repfactors: Vec<(&str, usize)> = Vec::new();

            for (i, (dc_name, dc_size)) in cluster_info.datacenter_sizes.iter().enumerate() {
                let to_substract = random_int([ks_id, i, 3434], 4);
                let mut cur_repfactor: usize = dc_size.checked_sub(to_substract).unwrap_or(1);
                if cur_repfactor == 0 {
                    cur_repfactor = 1;
                }

                if cur_repfactor > *dc_size {
                    cur_repfactor = *dc_size;
                }
                if cur_repfactor > max_replication_factor {
                    cur_repfactor = max_replication_factor;
                }

                dc_repfactors.push((dc_name, cur_repfactor));
            }

            // Sometimes drop 1 dc from the NTS, it doesn't have to contain all DCs.
            if dc_repfactors.len() > 1 && ks_id % 5 == 0 {
                let to_drop: usize = (ks_id * ks_id + 2323) % dc_repfactors.len();
                dc_repfactors.remove(to_drop);
            }

            let mut dc_repfactors_str: String = String::new();
            for (dc_name, dc_repfactor) in dc_repfactors {
                dc_repfactors_str += &format!(", '{dc_name}': {dc_repfactor}");
            }

            let query_str = format!("CREATE KEYSPACE bignts{ks_id} with replication = {{'class': 'NetworkTopologyStrategy'{dc_repfactors_str}}}");
            if verbose {
                println!("{ks_id}/{bignts_num}: {query_str}");
            } else {
                print!("{ks_id} ");
            }
            std::io::stdout().flush()?;
            session.query(query_str, ()).await?;
        }
        if !verbose {
            println!();
        }
        println!("Created");
    }

    Ok(())
}

async fn create_tables(session: &Session, verbose: bool) -> Result<()> {
    let keyspaces: Vec<String> = fetch_non_system_keyspaces(session, verbose).await?;

    let ks_num = keyspaces.len();
    println!("Creating test tables in {ks_num} keyspaces...");

    for (i, ks_name) in keyspaces.iter().enumerate() {
        std::io::stdout().flush()?;

        let query1 = format!(
            "CREATE TABLE {ks_name}.table1 (p int, c int, s int static, r int, PRIMARY KEY (p, c))"
        );
        let query2 = format!("CREATE TABLE {ks_name}.table2 (p1  int, p2 int, p3 int, c1 int, c2 int, c3 int, s int static, r1 int, r2 int, r3 int, PRIMARY KEY ((p1, p2, p3), c1, c2, c3))");
        if verbose && i < 3 {
            println!("{i}/{ks_num}: {query1}");
            println!("{i}/{ks_num}: {query2}");
        } else {
            print!("{i} ");
        }
        session.query(query1, ()).await?;
        session.query(query2, ()).await?;
    }

    println!("\nCreated the tables!");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::random_int;

    fn test_random_int_is_uniform(upper: usize) {
        let mut occurences: Vec<usize> = vec![0; upper];

        for i in 0..32 {
            for j in 0..32 {
                for k in 0..32 {
                    let random: usize = random_int([i, j, k], upper);
                    occurences[random] += 1;
                }
            }
        }

        let sum: usize = occurences.iter().sum();

        let mean = sum as f64 / occurences.len() as f64;

        for i in 0..occurences.len() {
            let deviation = (mean as i64 - occurences[i] as i64).abs() as f64;
            if deviation > 0.2 * mean {
                println!("{:?}", occurences);
                panic!(
                    "Mean is {:.2}, but {} had {} occurences. Allowed deviation: {:.2}",
                    mean,
                    i,
                    occurences[i],
                    0.2 * mean
                )
            }
        }
    }

    #[test]
    fn random_int_is_uniform() {
        for upper in [1, 2, 3, 4, 5, 6, 7].into_iter().chain((8..64).step_by(8)) {
            test_random_int_is_uniform(upper);
        }
    }
}
