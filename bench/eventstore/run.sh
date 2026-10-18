#!/usr/bin/env bash
# The event-store acceptance workload, run from a bench bundle (see
# bundle.sh). It runs on a load machine that has Docker and SSH access to the
# cluster machines, and for each committed build and source engine:
# starts the cluster, starts the source and read-model databases, runs
# eventstore-load, collects the results, and tears it all down.
#
#   ./run.sh [--labels a,b] [--engines postgres,mysql,sqlserver] [--profile]
#            [--pinned-labels a] [--profile-every SECONDS] [--save-log DEST]
#            [--from-log SRC] [-- eventstore-load flags]
#
# --save-log DEST, after a run with -history N -history-snapshot-only, takes
# a live backup of a follower and stores it with the history's dataset.json
# under DEST/<label>/: an s3:// prefix (the node uploads it with its instance
# role), or a local directory when the nodes are local.
#
# --from-log SRC restores every node from SRC/backup.tar (a prefix --save-log
# wrote, label included) as one new cluster instead of starting empty, and
# runs eventstore-load with -history-from-log SRC/dataset.json: it checks the
# log came back whole and builds over it. The source database starts empty.
#
# A label in --pinned-labels runs with node i in zone zi and the projections
# pinned to a follower's zone, so they sync off the leader. It names a build
# in bin/ like any other label (copy a build to a second name to run it both
# ways).
#
# Environment:
#   BENCH_NODES        cluster machine addresses, space-separated; "local" runs
#                      the nodes on this machine (BENCH_LOCAL_NODES, default 3)
#   BENCH_SSH_USER     SSH user on the cluster machines
#   BENCH_SSH_KEY      SSH key for them
#   BENCH_DATA_DIR     data directory on every machine (default ./local-data)
#   BENCH_RESULTS_DIR  where results go (default ./results)
#   BENCH_RUN_ID, BENCH_INSTANCE_TYPES   recorded in the results
#   BENCH_PROFILE      set (or pass --profile) to take a 30 s CPU profile and
#                      a 5 s execution trace from every node every
#                      BENCH_PROFILE_EVERY seconds (default 90) while the
#                      workload runs, into <results>/profiles/
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
labels=$(ls "$here/bin" | grep -v '^eventstore-load$' | paste -sd, -)
engines=postgres
pinned=
save_dest=
from_log=
while [ $# -gt 0 ]; do
  case $1 in
  --labels) labels=$2; shift 2 ;;
  --engines) engines=$2; shift 2 ;;
  --profile) BENCH_PROFILE=1; shift ;;
  --profile-every) BENCH_PROFILE_EVERY=$2; shift 2 ;;
  --pinned-labels) pinned=$2; shift 2 ;;
  --save-log) save_dest=${2%/}; shift 2 ;;
  --from-log) from_log=${2%/}; shift 2 ;;
  --) shift; break ;;
  *) echo "run.sh: unknown argument $1" >&2; exit 2 ;;
  esac
done
load_args=("$@")

DATA=${BENCH_DATA_DIR:-$here/local-data}
RESULTS=${BENCH_RESULTS_DIR:-$here/results}
NODES=${BENCH_NODES:-local}
PW=BenchPassw0rd2026 # throwaway databases on a throwaway rig
API_PORT=18080
PEER_PORT=18079
mkdir -p "$DATA" "$RESULTS"

if [ "$NODES" = local ]; then
  n=${BENCH_LOCAL_NODES:-3}
  hosts=()
  for i in $(seq 1 "$n"); do hosts+=(127.0.0.1); done
  src_host=127.0.0.1
else
  read -r -a hosts <<<"$NODES"
  src_host=$(hostname -I | awk '{print $1}')
fi
count=${#hosts[@]}

api_port() { if [ "$NODES" = local ]; then echo $((API_PORT + ($1 - 1) * 10000)); else echo $API_PORT; fi; }
peer_port() { if [ "$NODES" = local ]; then echo $((PEER_PORT + ($1 - 1) * 10000)); else echo $PEER_PORT; fi; }
node_dir() { if [ "$NODES" = local ]; then echo "$DATA/node$1"; else echo "$DATA/bench"; fi; }

# on_node i command: run a shell command on node i.
on_node() {
  local i=$1; shift
  if [ "$NODES" = local ]; then
    sh -c "$*"
  else
    ssh -i "$BENCH_SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
      "$BENCH_SSH_USER@${hosts[$((i - 1))]}" "$*"
  fi
}

install_committed() {
  local label=$1 i
  for i in $(seq 1 "$count"); do
    local dir; dir=$(node_dir "$i")
    on_node "$i" "mkdir -p '$dir'"
    if [ "$NODES" = local ]; then
      cp "$here/bin/$label/committed" "$dir/committed-bin"
    else
      scp -q -i "$BENCH_SSH_KEY" -o BatchMode=yes "$here/bin/$label/committed" \
        "$BENCH_SSH_USER@${hosts[$((i - 1))]}:$dir/committed-bin"
    fi
  done
}

peers() {
  local i out=
  for i in $(seq 1 "$count"); do
    out="$out${out:+,}$i=http://${hosts[$((i - 1))]}:$(peer_port "$i")"
  done
  echo "$out"
}

# start_nodes [zoned]: with zoned set, node i starts in zone zi. Each node
# starts empty, or on its restored data directory with --from-log.
start_nodes() {
  local zoned=${1:-} i p fresh; p=$(peers)
  for i in $(seq 1 "$count"); do
    local dir; dir=$(node_dir "$i")
    fresh="rm -rf '$dir/data' && mkdir -p '$dir/data' &&"
    [ -z "$from_log" ] || fresh=
    on_node "$i" "$fresh cd '$dir' || exit 1; \
      APP_DB_PASSWORD=$PW COMMITTED_NODE_ID=$i COMMITTED_API_ADDR=:$(api_port "$i") \
      COMMITTED_PEER_URL=http://${hosts[$((i - 1))]}:$(peer_port "$i") COMMITTED_PEERS=$p \
      COMMITTED_DATA_DIR='$dir/data' ${zoned:+COMMITTED_ZONE=z$i} ${BENCH_PROFILE:+COMMITTED_PPROF=true} nohup ./committed-bin node > node.log 2>&1 < /dev/null & echo \$! > '$dir/pid'"
  done
}

# fetch src name dest_dir [node]: copy src/name (an s3:// prefix or a local
# directory) into dest_dir, on node i when given, else here.
fetch() {
  local src=$1 name=$2 dest=$3 node=${4:-} cmd
  case $src in
  s3://*) cmd="aws s3 cp --only-show-errors '$src/$name' '$dest/$name'" ;;
  *) cmd="cp '$src/$name' '$dest/$name'" ;;
  esac
  if [ -n "$node" ]; then on_node "$node" "$cmd"; else sh -c "$cmd"; fi
}

# restore_nodes src out: every node restored from src's backup.tar as a
# member of one new cluster — node 1 mints the seed, the rest join it, as
# docs/operations/backup.md describes.
restore_nodes() {
  local src=$1 out=$2 i dir seed t0 pids=()
  t0=$(date +%s)
  for i in $(seq 1 "$count"); do
    dir=$(node_dir "$i")
    on_node "$i" "rm -rf '$dir/data' && mkdir -p '$dir'" || return 1
    fetch "$src" backup.tar "$dir" "$i" &
    pids+=($!)
  done
  for i in "${pids[@]}"; do wait "$i" || return 1; done
  echo "downloaded: $(($(date +%s) - t0))s" | tee "$out/restore.txt"
  t0=$(date +%s)
  dir=$(node_dir 1)
  seed=$(on_node 1 "cd '$dir' && ./committed-bin restore --from backup.tar --data '$dir/data' --node-id 1 --new-cluster && rm -f backup.tar" |
    tee -a "$out/restore.txt" | sed -n 's/.*--cluster-id \([0-9a-f-]*\).*/\1/p')
  [ -n "$seed" ] || { echo "run.sh: restore of node 1 printed no cluster seed" >&2; return 1; }
  pids=()
  for i in $(seq 2 "$count"); do
    dir=$(node_dir "$i")
    on_node "$i" "cd '$dir' && ./committed-bin restore --from backup.tar --data '$dir/data' --node-id $i --cluster-id $seed && rm -f backup.tar" >>"$out/restore.txt" &
    pids+=($!)
  done
  for i in ${pids[@]+"${pids[@]}"}; do wait "$i" || return 1; done
  echo "restored: $(($(date +%s) - t0))s" | tee -a "$out/restore.txt"
}

stop_nodes() {
  local i
  for i in $(seq 1 "$count"); do
    local dir; dir=$(node_dir "$i")
    on_node "$i" "if [ -f '$dir/pid' ]; then kill \$(cat '$dir/pid') 2>/dev/null; \
      for t in \$(seq 50); do kill -0 \$(cat '$dir/pid') 2>/dev/null || break; sleep 0.2; done; \
      kill -9 \$(cat '$dir/pid') 2>/dev/null; rm -f '$dir/pid'; fi; true" || true
  done
}

# collect_nodes dir: each node's log and the bytes in each area of its data
# directory (event log, raft log, metadata, projection stores).
collect_nodes() {
  local out=$1 i
  mkdir -p "$out/nodes"
  for i in $(seq 1 "$count"); do
    local dir; dir=$(node_dir "$i")
    on_node "$i" "du -sb '$dir/data'/* 2>/dev/null" >"$out/nodes/node$i.du" 2>/dev/null || true
    if [ "$NODES" = local ]; then
      cp "$dir/node.log" "$out/nodes/node$i.log" 2>/dev/null || true
    else
      scp -q -i "$BENCH_SSH_KEY" -o BatchMode=yes "$BENCH_SSH_USER@${hosts[$((i - 1))]}:$dir/node.log" \
        "$out/nodes/node$i.log" 2>/dev/null || true
    fi
  done
}

stop_dbs() { docker rm -f bench-src bench-read >/dev/null 2>&1 || true; }

wait_for() { # wait_for description command...
  local what=$1; shift
  for _ in $(seq 120); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
  echo "run.sh: $what not ready" >&2; return 1
}

# start_dbs engine: the read-model Postgres and the source, empty. Sets the
# connection settings for eventstore-load.
start_dbs() {
  local engine=$1
  stop_dbs
  docker run -d --name bench-read -e POSTGRES_USER=committed -e POSTGRES_PASSWORD=$PW -e POSTGRES_DB=app \
    -p 55433:5432 postgres:16 -c max_connections=300 >/dev/null
  read_dsn="postgres://committed:$PW@127.0.0.1:55433/app?sslmode=disable"
  read_url="postgres://committed:\${APP_DB_PASSWORD}@$src_host:55433/app?search_path=read&sslmode=disable"
  case $engine in
  postgres)
    docker run -d --name bench-src -e POSTGRES_USER=committed -e POSTGRES_PASSWORD=$PW -e POSTGRES_DB=app \
      -p 55432:5432 postgres:16 -c wal_level=logical -c max_connections=300 >/dev/null
    wait_for "source postgres" docker exec bench-src pg_isready -U committed -d app -h 127.0.0.1
    src_dsn="postgres://committed:$PW@127.0.0.1:55432/app?sslmode=disable"
    src_url="postgres://committed:\${APP_DB_PASSWORD}@$src_host:55432/app?sslmode=disable"
    ;;
  mysql)
    docker run -d --name bench-src -e MYSQL_ROOT_PASSWORD=$PW -e MYSQL_DATABASE=app -p 53306:3306 mysql:9 \
      --gtid-mode=ON --enforce-gtid-consistency=ON --binlog-row-metadata=FULL --max-connections=500 >/dev/null
    wait_for "source mysql" docker exec bench-src mysql -uroot -p$PW -h127.0.0.1 -e "SELECT 1" app
    src_dsn="root:$PW@tcp(127.0.0.1:53306)/app"
    src_url="mysql://root:\${APP_DB_PASSWORD}@$src_host:53306/app"
    ;;
  sqlserver)
    docker run -d --name bench-src --platform linux/amd64 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=$PW \
      -p 51433:1433 mcr.microsoft.com/mssql/server:2022-latest >/dev/null
    wait_for "source sql server" docker exec bench-src /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P $PW -Q "SELECT 1"
    docker exec bench-src /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P $PW -Q "CREATE DATABASE app" >/dev/null
    src_dsn="sqlserver://sa:$PW@127.0.0.1:51433?database=app"
    src_url="sqlserver://sa:\${APP_DB_PASSWORD}@$src_host:51433?database=app"
    ;;
  *) echo "run.sh: unknown engine $engine" >&2; return 2 ;;
  esac
  wait_for "read-model postgres" docker exec bench-read pg_isready -U committed -d app -h 127.0.0.1
}

# profile_nodes dir: every BENCH_PROFILE_EVERY seconds, a 30 s CPU profile
# and then a 5 s execution trace (where goroutines wait, which a CPU profile
# cannot show) from each node at once, named by node and seconds since the
# workload began. Runs until killed.
profile_nodes() {
  local dir=$1 start i t
  mkdir -p "$dir"
  start=$(date +%s)
  while true; do
    t=$(($(date +%s) - start))
    for i in $(seq 1 "$count"); do
      curl -s -o "$dir/node$i-t$t.pprof" "http://${hosts[$((i - 1))]}:$(api_port "$i")/debug/pprof/profile?seconds=30" &
    done
    wait
    for i in $(seq 1 "$count"); do
      curl -s -o "$dir/node$i-t$t.trace" "http://${hosts[$((i - 1))]}:$(api_port "$i")/debug/pprof/trace?seconds=5" &
    done
    wait
    sleep "$((${BENCH_PROFILE_EVERY:-90} - 35))"
  done
}

# save_log dest out: a live backup of a follower (the backup docs' advice),
# and the history's dataset.json from out, into dest.
save_log() {
  local dest=$1 out=$2 leader i src=1 dir
  leader=$(curl -s -m 10 "http://${hosts[0]}:$(api_port 1)/v1/node/status" | sed -n 's/.*"leader":\([0-9]*\).*/\1/p')
  for i in $(seq 1 "$count"); do
    if [ "$i" != "$leader" ]; then src=$i; break; fi
  done
  dir=$(node_dir "$src")
  echo "backing up node $src (leader ${leader:-unknown}) to $dest"
  local t0; t0=$(date +%s)
  on_node "$src" "cd '$dir' && ./committed-bin backup --live --target http://127.0.0.1:$(api_port "$src") --to '$dir/backup.tar'" || return 1
  on_node "$src" "ls -l '$dir/backup.tar'" | awk '{print "archive bytes:", $5}' | tee "$out/save.txt"
  echo "backup: $(($(date +%s) - t0))s" | tee -a "$out/save.txt"
  case $dest in
  s3://*)
    on_node "$src" "aws s3 cp --only-show-errors '$dir/backup.tar' '$dest/backup.tar' && rm -f '$dir/backup.tar'" || return 1
    aws s3 cp --only-show-errors "$out/dataset.json" "$dest/dataset.json" || return 1
    ;;
  *)
    [ "$NODES" = local ] || { echo "run.sh: --save-log needs an s3:// prefix for remote nodes" >&2; return 1; }
    mkdir -p "$dest" && mv "$dir/backup.tar" "$dest/backup.tar" && cp "$out/dataset.json" "$dest/dataset.json" || return 1
    ;;
  esac
  echo "saved: $(($(date +%s) - t0))s, $dest/backup.tar and dataset.json" | tee -a "$out/save.txt"
}

cleanup() { stop_nodes; stop_dbs; }
trap cleanup EXIT

nodes_flag=
for i in $(seq 1 "$count"); do nodes_flag="$nodes_flag${nodes_flag:+,}http://${hosts[$((i - 1))]}:$(api_port "$i")"; done

{
  echo "run ${BENCH_RUN_ID:-local} on $(uname -srm), $(nproc 2>/dev/null || sysctl -n hw.ncpu) cpus"
  echo "nodes: ${hosts[*]} (${BENCH_NODE_AZS:-})  load: ${BENCH_LOAD_AZ:-}  ${BENCH_INSTANCE_TYPES:-}"
} | tee "$RESULTS/environment.txt"

status=0
IFS=, read -r -a label_list <<<"$labels"
IFS=, read -r -a engine_list <<<"$engines"
for label in "${label_list[@]}"; do
  install_committed "$label"
  for engine in "${engine_list[@]}"; do
    out="$RESULTS/$label/$engine"
    mkdir -p "$out"
    echo "=== $label / $engine"
    "$here/bin/$label/committed" --version >"$out/version.txt" 2>&1 || true
    start_dbs "$engine"
    pin=()
    case ",$pinned," in *",$label,"*) pin=(-pin-follower) ;; esac
    restored=()
    if [ -n "$from_log" ]; then
      restore_nodes "$from_log" "$out" || { status=1; continue; }
      fetch "$from_log" dataset.json "$out" || { status=1; continue; }
      restored=(-history-from-log "$out/dataset.json")
    fi
    start_nodes ${pin[@]+zoned}
    profiler=
    if [ -n "${BENCH_PROFILE:-}" ]; then
      profile_nodes "$out/profiles" &
      profiler=$!
    fi
    set +e
    "$here/bin/eventstore-load" -nodes "$nodes_flag" -engine "$engine" -label "$label" \
      -recipe "$here/recipe/event-store-read-models.md" -results "$out" \
      -src-dsn "$src_dsn" -src-url "$src_url" -read-dsn "$read_dsn" -read-url "$read_url" \
      ${pin[@]+"${pin[@]}"} ${restored[@]+"${restored[@]}"} ${load_args[@]+"${load_args[@]}"} 2>&1 | tee "$out/load.log"
    rc=${PIPESTATUS[0]}
    set -e
    [ "$rc" -eq 0 ] || status=$rc
    if [ -n "$save_dest" ] && [ "$rc" -eq 0 ]; then
      save_log "$save_dest/$label" "$out" || status=1
    fi
    if [ -n "$profiler" ]; then kill "$profiler" 2>/dev/null; wait "$profiler" 2>/dev/null || true; fi
    stop_nodes
    collect_nodes "$out"
    docker logs bench-src >"$out/source.log" 2>&1 || true
    stop_dbs
  done
done
exit $status
