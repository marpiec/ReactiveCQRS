#!/usr/bin/env bash
# E2's fault tests (chapter 17), about 20 minutes. From the repository's root, after `sbt examples/stageE2`:
#   examples/deploy/e2/faults/run.sh
#
# Three nodes start under load (docker-compose.yml: each runs a client that keeps a ledger of its commands). Then, one
# after another, with the load going on:
#   1. the second oldest node is killed (SIGKILL), and started again a minute later;
#   2. the second oldest node is stopped gracefully (it hands its shards over), and started again: shards move twice;
#   3. the oldest node, which runs the carts list and the outbox poller, is cut off the other two while it still reaches
#      the database; the split brain resolver downs it, Docker starts it again, and it joins once the network is back;
#   4. the oldest node is killed: the carts list and the outbox poller move to the next oldest.
# The clients then stop, and `FaultTests check` compares the database with their ledger. Everything goes to
# examples/target/e2-faults/: run.log (the steps), outbox.log (what waits in the outbox, every 5 seconds), nodes.log,
# cluster-events.log (the nodes' cluster and singleton events, in time order) and check.log. Exits with the check's
# status. KEEP_RUNNING=1 leaves the containers up.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
out="$here/../../../target/e2-faults"
mkdir -p "$out"
rm -f "$out"/*.log

compose() { docker compose -f "$here/docker-compose.yml" "$@"; }
sql() { compose exec -T postgres psql -U shop -d shop -tAc "$1"; }
log() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$out/run.log"; }
container() { compose ps -aq "$1"; }

# The members a running node saw last, the oldest first, as node names.
members() { sql "SELECT members FROM fault_nodes WHERE seen > now() - interval '5 seconds' ORDER BY seen DESC LIMIT 1" | sed 's/peer/node/g'; }
oldest() { members | cut -d, -f1; }
second_oldest() { members | cut -d, -f2; }

# Until three nodes, all ready, see three members.
wait_ready() {
  for _ in $(seq 1 120); do
    if [ "$(sql "SELECT count(*) FROM fault_nodes WHERE seen > now() - interval '5 seconds' AND members LIKE '%,%,%'" 2>/dev/null || echo 0)" = "3" ]; then
      log "three nodes ready; members, the oldest first: $(members)"
      return 0
    fi
    sleep 5
  done
  log "the cluster did not form in 10 minutes"
  exit 2
}

state() { docker inspect -f '{{.State.Status}}, restarted {{.RestartCount}} times' "$(container "$1")"; }

log "starting PostgreSQL and three nodes"
compose --profile check down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d --build postgres node1 node2 node3
wait_ready

(while true; do
  echo "$(date -u +%H:%M:%S) $(sql "SELECT count(*) || ' waiting, the oldest for ' || coalesce(extract(epoch FROM now() AT TIME ZONE 'UTC' - min(event_time))::int, 0) || ' s' FROM events_to_publish" 2>/dev/null)" >> "$out/outbox.log"
  sleep 5
done) &
sampler=$!
trap 'kill $sampler 2>/dev/null || true' EXIT

log "load for 60 s"
sleep 60

victim=$(second_oldest)
log "1. kill $victim (SIGKILL)"
docker kill "$(container "$victim")" >/dev/null
sleep 5
log "1. $victim: $(state "$victim")"
sleep 55
log "1. start $victim again"
compose start "$victim" >/dev/null
wait_ready
sleep 30

victim=$(second_oldest)
log "2. stop $victim gracefully"
compose stop -t 120 "$victim" >/dev/null
log "2. $victim stopped"
sleep 20
log "2. start $victim again"
compose start "$victim" >/dev/null
wait_ready
sleep 60

victim=$(oldest)
log "3. cut $victim, the oldest, off the cluster network (it still reaches the database)"
docker network disconnect e2faults_cluster "$(container "$victim")"
sleep 45
log "3. $victim: $(state "$victim"); the others see: $(members)"
sleep 45
log "3. connect $victim again"
docker network connect --alias "${victim/node/peer}" e2faults_cluster "$(container "$victim")"
wait_ready
sleep 30

victim=$(oldest)
log "4. kill $victim, the oldest (SIGKILL)"
docker kill "$(container "$victim")" >/dev/null
sleep 60
log "4. the others see: $(members)"
log "4. start $victim again"
compose start "$victim" >/dev/null
wait_ready
sleep 30

log "stopping the clients; checking"
sql "UPDATE fault_control SET stopped = true" >/dev/null
set +e
compose --profile check run --rm check | tee "$out/check.log"
status=${PIPESTATUS[0]}
set -e

compose logs -t --no-color node1 node2 node3 > "$out/nodes.log"
grep -E "Singleton manager starting|Singleton actor .* was terminated|ClusterSingletonManager state change|SBR took decision|SBR is downing|Marking node as UNREACHABLE|Marking node as REACHABLE|Leader is removing|is ready|as the oldest|Running CoordinatedShutdown|Shutting down|Self downed|quarantined" "$out/nodes.log" \
  | sort -t'|' -k2 > "$out/cluster-events.log" || true
log "done: the check's exit status is $status; logs in examples/target/e2-faults"
if [ "${KEEP_RUNNING:-0}" != "1" ]; then compose --profile check down -v >/dev/null 2>&1; fi
exit "$status"
