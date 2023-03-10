use scylla::{
    statement::prepared_statement::PreparedStatement,
    transport::{topology::Strategy, ClusterData},
    Session, SessionBuilder,
};
use tokio::sync::Barrier;

use anyhow::Result;
use clap::Parser;
use stats_alloc::{Stats, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::{
    alloc::System,
    io::Write,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value = "127.0.0.1:9042")]
    node: String,

    #[clap(short, long, value_parser, default_value_t = 256usize)]
    parallelism: usize,

    #[clap(short, long, value_parser, default_value_t = 100_000usize)]
    requests: usize,
}

fn print_stats(stats: &stats_alloc::Stats) {
    println!("allocs:            {:9.2}", stats.allocations);
    println!("reallocs:          {:9.2}", stats.reallocations);
    println!("frees/req:             {:9.2}", stats.deallocations);
    println!("bytes allocated:   {:9.2}", stats.bytes_allocated);
    println!("bytes reallocated: {:9.2}", stats.bytes_reallocated);
    println!("bytes freed:       {:9.2}", stats.bytes_deallocated);
    println!(
        "allocated - freed: {:9.2}",
        stats.bytes_allocated as i64 - stats.bytes_deallocated as i64
    )
}

fn print_stats_per_req(stats: &stats_alloc::Stats, reqs: f64) {
    println!(
        "allocs/req:            {:9.2}",
        stats.allocations as f64 / reqs
    );
    println!(
        "reallocs/req:          {:9.2}",
        stats.reallocations as f64 / reqs
    );
    println!(
        "frees/req:             {:9.2}",
        stats.deallocations as f64 / reqs
    );
    println!(
        "bytes allocated/req:   {:9.2}",
        stats.bytes_allocated as f64 / reqs
    );
    println!(
        "bytes reallocated/req: {:9.2}",
        stats.bytes_reallocated as f64 / reqs
    );
    println!(
        "bytes freed/req:       {:9.2}",
        stats.bytes_deallocated as f64 / reqs
    );
    println!(
        "(allocated - freed)/req: {:9.2}",
        (stats.bytes_allocated as i64 - stats.bytes_deallocated as i64) as f64 / reqs
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    println!("{:?}", args);

    println!("Connecting to {} ...", args.node);

    let session: Session = SessionBuilder::new().known_node(&args.node).build().await?;
    let session = Arc::new(session);

    // Warmup Session by fetching the list of keyspaces a few times
    for _ in 0..4 {
        let _ = fetch_non_system_keyspaces(&session).await?;
    }

    measure_topology_refresh(&session).await?;

    measure_queries_benchmark(&session, KeyspaceType::Simple, &args).await?;
    measure_queries_benchmark(&session, KeyspaceType::Nts, &args).await?;

    Ok(())
}

async fn fetch_non_system_keyspaces(session: &Session) -> Result<Vec<String>> {
    let keyspaces: Vec<String> = session
        .query("SELECT keyspace_name FROM system_schema.keyspaces", ())
        .await?
        .rows_typed::<(String,)>()?
        .map(|res| res.map(|row| row.0))
        .map(|res| res.map_err(|r| r.into()))
        .collect::<Result<Vec<String>>>()?;

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

async fn measure_topology_refresh(session: &Session) -> Result<()> {
    let start_time: std::time::Instant = std::time::Instant::now();
    let initial_stats = GLOBAL.stats();

    session.refresh_metadata().await?;

    let refresh_duration: std::time::Duration = start_time.elapsed();
    let result_stats = GLOBAL.stats() - initial_stats;

    println!("Topology refresh took {:?}", refresh_duration);
    println!("Topology refresh stats:\n");
    print_stats(&result_stats);
    println!();

    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum KeyspaceType {
    Simple,
    Nts,
}

async fn measure_queries_benchmark(
    session: &Arc<Session>,
    ks_type: KeyspaceType,
    args: &Args,
) -> Result<()> {
    let ks_name: String = match find_ks_to_use(session, ks_type).await? {
        Some(ks_name) => {
            println!("Measuring queries for keyspace type: {ks_type:?} using keyspace {ks_name}");
            ks_name
        }
        None => {
            println!("No keyspace that can be used with keyspace type: {ks_type:?}, skipping.");
            return Ok(());
        }
    };

    let prepared_insert = session.prepare(format!("INSERT INTO {ks_name}.table2 (p1, p2, p3, c1, c2, c3, s, r1, r2, r3) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")).await?;
    let prepared_insert = Arc::new(prepared_insert);

    let prepared_select = session.prepare(format!("SELECT p1, p2, p3, c1, c2, c3, s, r1, r2, r3 FROM {ks_name}.table2 WHERE p1 = ? AND p2 = ? AND p3 = ? AND c1 = ? AND c2 = ? AND c3 = ?")).await?;
    let prepared_select = Arc::new(prepared_select);

    print!("Sending {} inserts, hold tight ", args.requests);
    let write_stats = measure(
        session.clone(),
        prepared_insert.clone(),
        10,
        args.requests,
        args.parallelism,
    )
    .await;
    println!("----------");
    println!("Inserts:");
    println!("----------");
    print_stats_per_req(&write_stats, args.requests as f64);
    println!("----------");

    print!("Sending {} selects, hold tight ", args.requests);
    let read_stats = measure(
        session.clone(),
        prepared_select.clone(),
        6,
        args.requests,
        args.parallelism,
    )
    .await;
    println!("----------");
    println!("Selects:");
    println!("----------");
    print_stats_per_req(&read_stats, args.requests as f64);
    println!("----------");

    Ok(())
}

async fn measure(
    session: Arc<Session>,
    prepared: Arc<PreparedStatement>,
    bind_variables_num: usize,
    reqs: usize,
    parallelism: usize,
) -> Stats {
    let barrier = Arc::new(Barrier::new(parallelism + 1));
    let counter = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::with_capacity(parallelism);
    for _ in 0..parallelism {
        let session = session.clone();
        let prepared = prepared.clone();
        let barrier = barrier.clone();
        let counter = counter.clone();
        tasks.push(tokio::task::spawn(async move {
            barrier.wait().await;
            barrier.wait().await;

            let mut bind_values_vec: Vec<i32> = vec![0; bind_variables_num];

            loop {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                if i >= reqs {
                    break;
                }
                if i % 10000 == 0 {
                    print!(".");
                    std::io::stdout().flush().unwrap();
                }
                for x in 0..bind_values_vec.len() {
                    bind_values_vec[x] = (i + x) as i32;
                }
                session
                    .execute(&prepared, bind_values_vec.as_slice())
                    .await
                    .unwrap();
            }

            barrier.wait().await;
            barrier.wait().await;
        }));
    }

    barrier.wait().await;
    let initial_stats = GLOBAL.stats();
    barrier.wait().await;

    barrier.wait().await;
    let final_stats = GLOBAL.stats();
    barrier.wait().await;

    // Wait until all tasks are cleaned up
    for t in tasks {
        t.await.unwrap();
    }

    println!();

    final_stats - initial_stats
}

async fn find_ks_to_use(session: &Session, ks_type: KeyspaceType) -> Result<Option<String>> {
    let ks_names: Vec<String> = fetch_non_system_keyspaces(session).await?;

    let cluster_data: Arc<ClusterData> = session.get_cluster_data();

    for ks_name in ks_names {
        let ks_info = match cluster_data.get_keyspace_info().get(&ks_name) {
            Some(info) => info,
            None => {
                panic!("No info for keyspace {ks_name}!");
            }
        };

        match (&ks_info.strategy, &ks_type) {
            (Strategy::SimpleStrategy { .. }, KeyspaceType::Simple) => return Ok(Some(ks_name)),
            (Strategy::NetworkTopologyStrategy { .. }, KeyspaceType::Nts) => {
                return Ok(Some(ks_name))
            }
            _ => continue,
        };
    }

    Ok(None)
}
