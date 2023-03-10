set -e

# Workload options:
# simple: A few SimpleStrategy keyspaces with RF = 1..=5
# bigsimple: simple + a SimpleStrategy with RF=nodes_num
# nts: NetworkTopolgyStrategy, keyspaces where each datacenter has RF=1..5
# bignts: nts + each datacenter has keyspaces with RF=nodes_in_dc_num
# combined - all of the above

WORKLOAD=${1:-simple}

CUR_TIME=$(date +"%Y-%m-%d-%H-%M-%S") 
OUTPUT_DIR=${2:-"run-$CUR_TIME"}

mkdir "$OUTPUT_DIR"

# Perform an initial clean & setup to keep the logs clean
cargo run --bin setup -- "$WORKLOAD" -v | tee "$OUTPUT_DIR/initsetup.txt"

cargo run --bin setup -- "$WORKLOAD" -v | tee "$OUTPUT_DIR/oldsetup.txt"
RUST_LOG=info cargo run --release --bin oldbench | tee "$OUTPUT_DIR/oldlog.txt"

cargo run --bin setup -- "$WORKLOAD" -v | tee "$OUTPUT_DIR/newsetup.txt"
RUST_LOG=info cargo run --release --bin newbench | tee "$OUTPUT_DIR/newlog.txt"

echo "Difference between setup outputs should be minimal:"
diff "$OUTPUT_DIR/oldsetup.txt" "$OUTPUT_DIR/newsetup.txt" | tee "$OUTPUT_DIR/diffsetup.txt"
