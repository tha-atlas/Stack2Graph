#!/bin/sh
# Derive bounded settings from the container's actual memory and CPU limits.
set -eu
[ "$(id -u)" -ne 0 ] || { echo 'PostgreSQL must run as the invoking non-root host user' >&2; exit 1; }
memory_kb=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)
for limit_file in /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory/memory.limit_in_bytes; do
    if [ -r "$limit_file" ]; then
        limit=$(cat "$limit_file")
        case "$limit" in ''|*[!0-9]*) continue ;; esac
        case "$limit_file" in
            */memory.max) used_file=/sys/fs/cgroup/memory.current ;;
            *) used_file=/sys/fs/cgroup/memory/memory.usage_in_bytes ;;
        esac
        used=0
        [ ! -r "$used_file" ] || used=$(cat "$used_file")
        limit_kb=$(( (limit - used) / 1024 ))
        [ "$limit_kb" -ge "$memory_kb" ] || memory_kb=$limit_kb
    fi
done
[ "$memory_kb" -ge 131072 ] || { echo 'PostgreSQL requires at least 128 MiB available RAM' >&2; exit 1; }
cpu=$(nproc)
if [ -r /sys/fs/cgroup/cpu.max ]; then
    read -r quota period < /sys/fs/cgroup/cpu.max
    if [ "$quota" != max ]; then
        quota_cpu=$(( (quota + period - 1) / period ))
        [ "$quota_cpu" -ge "$cpu" ] || cpu=$quota_cpu
    fi
fi
if [ -r /sys/fs/cgroup/cpu/cpu.cfs_quota_us ]; then
    quota=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us)
    period=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us)
    if [ "$quota" -gt 0 ]; then
        quota_cpu=$(( (quota + period - 1) / period ))
        [ "$quota_cpu" -ge "$cpu" ] || cpu=$quota_cpu
    fi
fi
connections=$((cpu * 4 + 36))
[ "$connections" -le 128 ] || connections=128
memory_connections=$((memory_kb / 8192))
[ "$connections" -le "$memory_connections" ] || connections=$memory_connections
[ "$connections" -ge 8 ] || connections=8
connections=${STACK2GRAPH_POSTGRES_MAX_CONNECTIONS:-$connections}
shared=$((memory_kb / 4))
maintenance=$((memory_kb / 16))
[ "$maintenance" -le 1048576 ] || maintenance=1048576
work=$((memory_kb / (connections * 32)))
[ "$work" -ge 64 ] || work=64
[ "$work" -le 32768 ] || work=32768
workers=$cpu
[ "$workers" -le 32 ] || workers=32
maintenance_workers=$((workers / 2))
[ "$maintenance_workers" -le 8 ] || maintenance_workers=8
free_kb=$(df -Pk /var/lib/postgresql | awk 'NR==2 {print $4}')
wal=$((free_kb / 16))
[ "$wal" -le 4194304 ] || wal=4194304
[ "$wal" -ge 131072 ] || wal=131072
spill=$((free_kb / (connections * 2)))
[ "$spill" -le 8388608 ] || spill=8388608
exec /usr/local/bin/docker-entrypoint.sh postgres \
    -c "shared_buffers=${STACK2GRAPH_POSTGRES_SHARED_BUFFERS:-${shared}kB}" \
    -c "maintenance_work_mem=${STACK2GRAPH_POSTGRES_MAINTENANCE_MEMORY:-${maintenance}kB}" \
    -c "work_mem=${STACK2GRAPH_POSTGRES_WORK_MEMORY:-${work}kB}" \
    -c "max_connections=$connections" \
    -c "max_worker_processes=$((workers + 8))" \
    -c "max_parallel_workers=$workers" \
    -c "max_parallel_workers_per_gather=0" \
    -c "max_parallel_maintenance_workers=$maintenance_workers" \
    -c "max_wal_size=${STACK2GRAPH_POSTGRES_MAX_WAL_SIZE:-${wal}kB}" \
    -c "min_wal_size=128MB" \
    -c "temp_file_limit=${STACK2GRAPH_POSTGRES_TEMP_FILE_LIMIT:-${spill}kB}" \
    -c "log_statement=none" -c "log_min_error_statement=panic" \
    -c "log_parameter_max_length=0" -c "log_parameter_max_length_on_error=0" \
    -c "password_encryption=scram-sha-256"
