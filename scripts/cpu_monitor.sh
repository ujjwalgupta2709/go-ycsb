#!/bin/bash
# cpu_monitor.sh — Capture processes using >THRESHOLD% CPU over time.
# Usage: ./cpu_monitor.sh [threshold] [interval_sec] [output_file]
#   threshold    : minimum CPU% to log (default: 25)
#   interval_sec : polling interval in seconds (default: 3)
#   output_file  : CSV log file (default: cpu_log_<timestamp>.csv)
#
# Run in background:  ./cpu_monitor.sh 25 5 &
# Stop:               kill %1   (or Ctrl+C if foreground)
# View summary:       printed on exit automatically

THRESHOLD="${1:-25}"
INTERVAL="${2:-3}"
LOGFILE="${3:-cpu_log_$(date +%Y%m%d_%H%M%S).csv}"

echo "CPU monitor started: threshold=${THRESHOLD}%, interval=${INTERVAL}s, log=${LOGFILE}"
echo "timestamp,pid,user,cpu_pct,command" > "$LOGFILE"

cleanup() {
    echo ""
    echo "=== CPU Monitor Summary ==="
    echo ""
    if [ ! -s "$LOGFILE" ] || [ "$(wc -l < "$LOGFILE")" -le 1 ]; then
        echo "No processes exceeded ${THRESHOLD}% CPU during monitoring."
        exit 0
    fi

    # Print per-process summary: PID, command, sample count, first/last seen, avg/max CPU
    echo "PID | Command | Samples | First Seen | Last Seen | Avg CPU% | Max CPU%"
    echo "--- | ------- | ------- | ---------- | --------- | -------- | --------"

    tail -n +2 "$LOGFILE" | awk -F',' '
    {
        pid = $2
        cpu = $4 + 0
        cmd = $5
        ts  = $1

        count[pid]++
        sum[pid] += cpu
        if (!(pid in max) || cpu > max[pid]) max[pid] = cpu
        if (!(pid in first)) first[pid] = ts
        last[pid] = ts
        name[pid] = cmd
    }
    END {
        for (pid in count) {
            avg = sum[pid] / count[pid]
            printf "%s | %s | %d | %s | %s | %.1f | %.1f\n",
                pid, name[pid], count[pid], first[pid], last[pid], avg, max[pid]
        }
    }' | sort -t'|' -k6 -rn

    echo ""
    echo "Full log: $LOGFILE"
    exit 0
}

trap cleanup INT TERM

while true; do
    ts=$(date '+%Y-%m-%d %H:%M:%S')
    # ps aux: USER PID %CPU %MEM ... COMMAND
    # Skip header, filter by CPU threshold, log matching rows
    ps aux --sort=-%cpu | awk -v ts="$ts" -v thresh="$THRESHOLD" '
        NR > 1 {
            cpu = $3 + 0
            if (cpu >= thresh) {
                cmd = $11
                for (i = 12; i <= NF; i++) cmd = cmd " " $i
                # Skip our own ps command
                if (cmd ~ /ps aux/) next
                printf "%s,%s,%s,%.1f,%s\n", ts, $2, $1, cpu, cmd
            }
        }
    ' >> "$LOGFILE"
    sleep "$INTERVAL"
done
