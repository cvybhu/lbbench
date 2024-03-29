use anyhow::Result;
use clap::{ArgEnum, Parser};
use rand::{Rng, SeedableRng};
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
    datacenter_sizes: Vec<(String, usize)>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let session: Session = SessionBuilder::new()
        .known_node(args.node)
        .auto_schema_agreement_timeout(std::time::Duration::from_secs(3600))
        .build()
        .await?;

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

    let mut datacenter_sizes_vec: Vec<(String, usize)> = datacenter_sizes.into_iter().collect();
    datacenter_sizes_vec.sort();

    let cluster_info = ClusterInfo {
        total_nodes,
        datacenter_sizes: datacenter_sizes_vec,
    };

    if verbose {
        println!("Got ClusterInfo:\n{:#?}", cluster_info);
    }

    Ok(cluster_info)
}

async fn generate_keyspaces(
    session: &Session,
    cluster_info: &ClusterInfo,
    mode: Mode,
    verbose: bool,
    max_replication_factor: usize,
) -> Result<()> {
    if mode == Mode::Simple || mode == Mode::Bigsimple || mode == Mode::Combined {
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        let simple_num = 20;
        println!("Creating {simple_num} SimpleStrategy keyspaces with RF = 1..=8");

        for ks_id in 0..simple_num {
            let repfactor_upper = [8, cluster_info.total_nodes, max_replication_factor]
                .into_iter()
                .min()
                .unwrap();
            let repfactor = rng.gen_range(1..=repfactor_upper);
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
        let mut rng = rand::rngs::StdRng::seed_from_u64(2);
        let bigsimple_num = 8;
        println!(
            "Creating {} SimpleStrategy keyspaces with RF~{}",
            bigsimple_num, cluster_info.total_nodes
        );

        for ks_id in 0..bigsimple_num {
            let upper = std::cmp::min(cluster_info.total_nodes, max_replication_factor);
            let lower = if upper < 9 { 1 } else { upper - 8 };
            let repfactor = rng.gen_range(lower..=upper);

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
        let mut rng = rand::rngs::StdRng::seed_from_u64(3);
        let nts_num = 32;

        println!("Creating {nts_num} NetworkTopologyStrategy keyspaces with RF=1..=8");

        for ks_id in 0..nts_num {
            let mut dc_repfactors: Vec<(&str, usize)> = Vec::new();

            for (dc_name, dc_size) in &cluster_info.datacenter_sizes {
                let repfactor_upper = [8, *dc_size, max_replication_factor]
                    .into_iter()
                    .min()
                    .unwrap();
                let cur_repfactor = rng.gen_range(1..=repfactor_upper);

                dc_repfactors.push((dc_name.as_str(), cur_repfactor));
            }

            // Sometimes drop 1 dc from the NTS, it doesn't have to contain all DCs.
            if dc_repfactors.len() > 1 && rng.gen_range(0..5) == 0 {
                let to_drop: usize = rng.gen_range(0..dc_repfactors.len());
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
        let mut rng = rand::rngs::StdRng::seed_from_u64(4);

        let bignts_num = 8;
        println!("Creating {bignts_num} NetworkTopologyStrategy keyspaces with RF~dcsize");

        for ks_id in 0..bignts_num {
            let mut dc_repfactors: Vec<(&str, usize)> = Vec::new();

            for (dc_name, dc_size) in cluster_info.datacenter_sizes.iter() {
                let upper = std::cmp::min(*dc_size, max_replication_factor);
                let lower = if upper < 9 { 1 } else { upper - 8 };
                let cur_repfactor = rng.gen_range(lower..=upper);

                dc_repfactors.push((dc_name.as_str(), cur_repfactor));
            }

            // Sometimes drop 1 dc from the NTS, it doesn't have to contain all DCs.
            if dc_repfactors.len() > 1 && rng.gen_range(0..5) == 0 {
                let to_drop: usize = rng.gen_range(0..dc_repfactors.len());
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
