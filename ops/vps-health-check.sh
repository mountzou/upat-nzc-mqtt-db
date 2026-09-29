#!/usr/bin/env bash
set -u -o pipefail
# Read-only VPS health report. Run locally on the VPS.
ROOT_FS=${ROOT_FS:-/}; DB_MOUNT=${DB_MOUNT:-/mnt/HC_Volume_106884142}; DB_PATH=${DB_PATH:-/mnt/HC_Volume_106884142/pgdata}
DB_CONTAINER=${DB_CONTAINER:-iot_postgres}; DB_NAME=${POSTGRES_DB:-iot_db}; DB_USER=${POSTGRES_USER:-postgres}
EXPECTED_CONTAINERS=(iot_api iot_caddy iot_postgres iot_mosquitto shelly_ingestor ttn_ingestor)
EXPECTED_TIMERS=(upat-aggregate-energy.timer upat-pv-ingestor.timer upat-journal-vacuum.timer upat-iaq-notifications-hourly.timer upat-iaq-notifications-daily.timer)
# Excluded from expected-device freshness checks after the 2026-09-20
# read-only audit: these catalog entries are retired/non-operational and have
# no observed measurements in the audited history window.
EXCLUDED_DEVICE_IDS=(portable-101 portable-104 portable-105 shellyplugsg3-8cbfeaa29930 shellypro3em-ac15187c76cc)
EXCLUDED_DEVICE_REASON='retired/non-operational; no observed measurements in audit window'
green='🟢 Healthy'; yellow='🟡 Warning'; red='🔴 Needs attention'; grey='⚪ Unknown'; overall=0; has_unknown=0
raise_status(){ (( $1 > overall )) && overall=$1; }
raise_unknown(){ has_unknown=1; }
pct_status(){ local n=$1; ((n>80))&&printf '%s' "$red"&&return; ((n>=70))&&printf '%s' "$yellow"&&return; printf '%s' "$green"; }
# Shelly plugs publish about every 60s; Pro3EM about every 7-16s.
# Compare exact seconds so rounding cannot hide a threshold crossing.
shelly_freshness_status() {
  local seconds=$1
  if [[ ! "$seconds" =~ ^[0-9]+$ ]]; then printf '%s' "$grey"
  elif ((seconds > 900)); then printf '%s' "$red"
  elif ((seconds > 300)); then printf '%s' "$yellow"
  else printf '%s' "$green"
  fi
}
command_exists(){ command -v "$1" >/dev/null 2>&1; }
printf '# VPS read-only health report\n\nGenerated: %s\n\n' "$(date -Is)"
printf '## 1. Filesystem and volumes\n\n| Root file system | File system path | Total | Used | Free | In-use | Inodes used | Mount | Status |\n|---|---|---:|---:|---:|---:|---:|---|---|\n'
for mount in "$ROOT_FS" "$DB_MOUNT"; do
 line=$(df -P -h "$mount" 2>/dev/null|tail -n1); inode=$(df -P -i "$mount" 2>/dev/null|tail -n1)
 if [[ -z "$line"||-z "$inode" ]]; then printf '| unknown | %s | — | — | — | — | — | unavailable | %s |\n' "$mount" "$grey"; raise_unknown; continue; fi
 read -r fs total used free use mountpoint <<<"$line"; ipct=$(awk '{print $5}'<<<"$inode"); pct=${use%%%}; ip=${ipct%%%}; status=$(pct_status "$pct"); ((ip>80))&&status=$red||{ ((ip>=70))&&status=$yellow; }; ((pct>80||ip>80))&&raise_status 2||{ ((pct>=70||ip>=70))&&raise_status 1; }; path="$mountpoint"; [[ "$mount" == "$DB_MOUNT" ]]&&path="$DB_PATH"
 printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' "$fs" "$path" "$total" "$used" "$free" "$use" "$ipct" "$mountpoint" "$status"
done
printf '\n## 2. CPU and system load\n\n'; vcpus=$(nproc 2>/dev/null||printf 0); read -r load1 load5 load15 _ </proc/loadavg 2>/dev/null||read -r load1 load5 load15<<<'unknown unknown unknown'; ratio=$(awk -v l="$load5" -v c="$vcpus" 'BEGIN{if(c>0)printf "%.2f",l/c;else print "unknown"}'); cpu_status=$grey; [[ "$ratio" != unknown ]]&&cpu_status=$green
if [[ "$ratio" != unknown ]]; then awk -v r="$ratio" 'BEGIN{exit !(r>1)}' && { cpu_status=$red; raise_status 2; } || { awk -v r="$ratio" 'BEGIN{exit !(r>=0.7)}' && { cpu_status=$yellow; raise_status 1; }; }; fi
cpu_sample() { awk '/^cpu / {idle=$5+$6; total=0; for(i=2;i<=NF;i++) total+=$i; print total, idle, $6; exit}' /proc/stat; }
read -r total_a idle_a io_a <<<"$(cpu_sample)"; sleep 1; read -r total_b idle_b io_b <<<"$(cpu_sample)"
cpu_used=unknown; io_wait=unknown
if [[ "$total_a" =~ ^[0-9]+$ && "$total_b" =~ ^[0-9]+$ && "$total_b" -gt "$total_a" ]]; then
  cpu_used=$(awk -v t="$total_b" -v ta="$total_a" -v i="$idle_b" -v ia="$idle_a" 'BEGIN{printf "%.0f%%",((t-ta)-(i-ia))/(t-ta)*100}')
  io_wait=$(awk -v t="$total_b" -v ta="$total_a" -v i="$io_b" -v ia="$io_a" 'BEGIN{printf "%.0f%%",(i-ia)/(t-ta)*100}')
fi
cpu_pct=${cpu_used%%%}; io_pct=${io_wait%%%}
if [[ "$cpu_used" != unknown ]]; then ((cpu_pct>90))&&{ cpu_status=$red; raise_status 2; }||{ ((cpu_pct>=80))&&{ [[ "$cpu_status" == "$green" ]]&&cpu_status=$yellow; raise_status 1; }; }; fi
if [[ "$io_wait" != unknown ]]; then ((io_pct>20))&&{ cpu_status=$red; raise_status 2; }||{ ((io_pct>=10))&&{ [[ "$cpu_status" == "$green" ]]&&cpu_status=$yellow; raise_status 1; }; }; fi
[[ "$ratio" == unknown || "$cpu_used" == unknown || "$io_wait" == unknown ]] && raise_unknown
printf '| vCPUs | Load 1m | Load 5m | Load 15m | Load / vCPU | CPU used | I/O wait | Status |\n|---:|---:|---:|---:|---:|---:|---:|---|\n| %s | %s | %s | %s | %s | %s | %s | %s |\n' "$vcpus" "$load1" "$load5" "$load15" "$ratio" "$cpu_used" "$io_wait" "$cpu_status"
printf '\n## 3. Memory and swap\n\n'; mem=$(free -h 2>/dev/null||true); read -r _ mt mu _ _ _ ma<<<"$(awk '/^Mem:/{print}'<<<"$mem")"; read -r _ st su sf<<<"$(awk '/^Swap:/{print}'<<<"$mem")"; mpct=$(awk '/^MemTotal:/{t=$2}/^MemAvailable:/{a=$2}END{if(t>0)printf "%.0f%%",(t-a)/t*100;else print "unknown"}' /proc/meminfo); spct=$(awk '/^SwapTotal:/{t=$2}/^SwapFree:/{f=$2}END{if(t>0)printf "%.0f%%",(t-f)/t*100;else print "0%"}' /proc/meminfo)
mem_status=$green; mp=${mpct%%%}; sp=${spct%%%}; if [[ "$mpct" != unknown ]]; then ((mp>85))&&{ mem_status=$red; raise_status 2; }||{ ((mp>=70))&&{ mem_status=$yellow; raise_status 1; }; }; fi; if ((sp>25)); then mem_status=$red; raise_status 2; elif ((sp>=10)); then [[ "$mem_status" == "$green" ]]&&mem_status=$yellow; raise_status 1; fi
oom_status=unknown; if command_exists journalctl; then oom_hits=$(timeout 5s journalctl -k -b --no-pager -q -g 'oom|out of memory|killed process' 2>/dev/null || true); [[ -z "$oom_hits" ]] && oom_status=0 || oom_status=1; else raise_unknown; fi; [[ "$mpct" == unknown || "$spct" == unknown || "$oom_status" == unknown ]] && raise_unknown; [[ "$oom_status" == 1 ]] && { mem_status=$red; raise_status 2; }
printf '| Memory total | Memory used | Memory available | Memory in-use | Swap total | Swap used | Swap free | Swap in-use | OOM events | Status |\n|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' "${mt:-unknown}" "${mu:-unknown}" "${ma:-unknown}" "${mpct:-unknown}" "${st:-unknown}" "${su:-unknown}" "${sf:-unknown}" "${spct:-unknown}" "${oom_status:-unknown}" "$mem_status"
printf '\n## 4. Docker runtime and containers\n\n| Container / runtime | State | Health / result | Restarts | Uptime | Role | Status |\n|---|---|---|---:|---|---|---|\n'; docker_ok=0; command_exists docker&&timeout 10s docker info >/dev/null 2>&1&&docker_ok=1
if ((docker_ok));then printf '| Docker daemon | available | API responds | — | — | Runtime | %s |\n' "$green";else printf '| Docker daemon | unavailable | — | — | — | Runtime | %s |\n' "$red";raise_status 2;fi
for c in "${EXPECTED_CONTAINERS[@]}";do if ((docker_ok))&&timeout 5s docker inspect "$c" >/dev/null 2>&1;then state=$(timeout 5s docker inspect -f '{{.State.Status}}' "$c" || printf unknown); health=$(timeout 5s docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}' "$c" || printf unknown); restarts=$(timeout 5s docker inspect -f '{{.RestartCount}}' "$c" || printf unknown); up=$(timeout 5s docker inspect -f '{{.State.StartedAt}}' "$c" || printf unknown); cs=$green; [[ "$state" == unknown || "$health" == unknown ]]&&{ cs=$grey; raise_unknown; }; [[ "$state" != running && "$state" != unknown || "$health" == unhealthy ]]&&cs=$red; printf '| %s | %s | %s | %s | %s | Production | %s |\n' "$c" "$state" "$health" "$restarts" "$up" "$cs"; [[ "$cs" == "$red" ]]&&raise_status 2;else printf '| %s | missing | — | — | — | Production | %s |\n' "$c" "$red";raise_status 2;fi;done
printf '\n## 5. PostgreSQL\n\n| PostgreSQL | Container | Readiness | Read-only query | Connections | Long queries | Blocked queries | Data mount | Status |\n|---|---|---|---|---:|---:|---:|---|---|\n'; pgstate=unknown;pghealth=unknown;pgready=unknown;pgquery=unknown;mountok=unknown;connections=unknown
long_queries=unknown; blocked_queries=unknown
if ((docker_ok))&&timeout 5s docker inspect "$DB_CONTAINER" >/dev/null 2>&1;then pgstate=$(timeout 5s docker inspect -f '{{.State.Status}}' "$DB_CONTAINER" || printf unknown);pghealth=$(timeout 5s docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}' "$DB_CONTAINER" || printf unknown);timeout 5s docker exec "$DB_CONTAINER" pg_isready -U "$DB_USER" -d "$DB_NAME" >/dev/null 2>&1&&pgready=accepting||pgready=failed;timeout 5s docker exec "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -Atqc 'SELECT 1' 2>/dev/null|grep -qx 1&&pgquery=OK||pgquery=failed;mountok=$(timeout 5s docker inspect -f '{{range .Mounts}}{{if eq .Destination "/var/lib/postgresql/data"}}{{.Source}}{{end}}{{end}}' "$DB_CONTAINER" || printf unknown);[[ "$mountok" == "$DB_PATH" ]]&&mountok=correct||mountok=unexpected;connections=$(timeout 5s docker exec "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -Atqc "SELECT count(*) || '/' || current_setting('max_connections') FROM pg_stat_activity" 2>/dev/null||printf unknown);long_queries=$(timeout 5s docker exec "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -Atqc "SELECT count(*) FROM pg_stat_activity WHERE state='active' AND query_start < now()-interval '5 minutes'" 2>/dev/null||printf unknown);blocked_queries=$(timeout 5s docker exec "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -Atqc "SELECT count(*) FROM pg_stat_activity a WHERE cardinality(pg_blocking_pids(a.pid))>0" 2>/dev/null||printf unknown);fi
pgs=$green;[[ "$pgstate" != running||"$pgready" != accepting||"$pgquery" != OK||"$mountok" != correct ]]&&pgs=$red;[[ "$connections" == unknown || "$long_queries" == unknown || "$blocked_queries" == unknown ]]&&{ [[ "$pgs" == "$green" ]]&&pgs=$grey; raise_unknown; };conn_used=${connections%/*};conn_max=${connections#*/};if [[ "$conn_used" =~ ^[0-9]+$ && "$conn_max" =~ ^[0-9]+$ && "$conn_max" -gt 0 ]];then conn_pct=$((conn_used*100/conn_max));((conn_pct>85))&&pgs=$red&&raise_status 2||{ ((conn_pct>=70))&&[[ "$pgs" == "$green" ]]&&pgs=$yellow&&raise_status 1;};fi;[[ "$long_queries" =~ ^[1-9] ]]&&pgs=$yellow&&raise_status 1;[[ "$blocked_queries" =~ ^[1-9] ]]&&pgs=$red&&raise_status 2;printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' "$DB_NAME" "$pgstate/$pghealth" "$pgready" "$pgquery" "$connections" "$long_queries" "$blocked_queries" "$mountok" "$pgs"
printf '\n## 6. Data freshness by expected device\n\n| Source | Device | Last event (UTC) | Age | Events 24h | Events 7d | Status | Reason |\n|---|---|---|---:|---:|---:|---|---|\n'
# Catalog-driven expected devices. Retired/non-operational devices remain visible
# as Excluded and never raise the aggregate health status.
freshness_rows=''
if ((docker_ok)) && timeout 5s docker inspect "$DB_CONTAINER" >/dev/null 2>&1; then
  freshness_rows=$(timeout 90s docker exec -e PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=80000 -c lock_timeout=2000" "$DB_CONTAINER" psql -X -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME" -At -F $'\t' -c "
    WITH upat_stats AS (
      SELECT device_id::text, max(event_time) AS last_event,
             count(*) FILTER (WHERE event_time >= now()-interval '24 hours') AS events_24h,
             count(*) FILTER (WHERE event_time >= now()-interval '7 days') AS events_7d
      FROM upat_measurements GROUP BY device_id
    ), shelly_stats AS (
      -- Compact-only writer: never fall back to frozen legacy measurements.
      -- Indexed latest lookup preserves the actual timestamp for stale devices;
      -- counts scan only the recent seven-day range of each series.
      SELECT s.device_id::text, max(latest.event_time) AS last_event,
             sum(recent.events_24h) AS events_24h, sum(recent.events_7d) AS events_7d
      FROM shelly_compact.series s
      LEFT JOIN LATERAL (
        SELECT m.event_time FROM shelly_compact.measurements m
        WHERE m.series_id=s.series_id AND m.event_time IS NOT NULL
        ORDER BY m.event_time DESC LIMIT 1
      ) latest ON true
      LEFT JOIN LATERAL (
        SELECT count(*) FILTER (WHERE m.event_time >= now()-interval '24 hours') AS events_24h,
               count(*) AS events_7d
        FROM shelly_compact.measurements m
        WHERE m.series_id=s.series_id AND m.event_time >= now()-interval '7 days'
      ) recent ON true
      GROUP BY s.device_id
    ), pv_stats AS (
      SELECT device_id, max(observed_at) AS last_event,
             count(*) FILTER (WHERE observed_at >= now()-interval '24 hours') AS events_24h,
             count(*) FILTER (WHERE observed_at >= now()-interval '7 days') AS events_7d
      FROM pv_device_readings_5m GROUP BY device_id
    ), expected AS (
      SELECT 'UPAT'::text AS source, d.device_id::text, s.last_event, coalesce(s.events_24h,0) events_24h, coalesce(s.events_7d,0) events_7d
      FROM upat_devices d LEFT JOIN upat_stats s ON s.device_id=d.device_id
      UNION ALL
      SELECT 'Shelly', d.device_id::text, s.last_event, coalesce(s.events_24h,0), coalesce(s.events_7d,0)
      FROM shelly_devices d LEFT JOIN shelly_stats s ON s.device_id=d.device_id
      UNION ALL
      SELECT 'PV', coalesce(d.provider_device_id::text,d.id::text) || ' (' || coalesce(d.device_role,'unknown') || ')',
             s.last_event, coalesce(s.events_24h,0), coalesce(s.events_7d,0)
      FROM pv_devices d LEFT JOIN pv_stats s ON s.device_id=d.id
      WHERE d.is_active IS TRUE
    )
    SELECT source, device_id, coalesce(to_char(last_event AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'),'unknown'),
           CASE WHEN last_event IS NULL THEN 'unknown'
                WHEN source='Shelly' THEN ceil(extract(epoch FROM (now()-last_event)))::text
                ELSE floor(extract(epoch FROM (now()-last_event))/3600)::text END,
           events_24h::text, events_7d::text
    FROM expected ORDER BY source, device_id
  " 2>/dev/null) || freshness_rows=''
fi
if [[ -z "$freshness_rows" ]]; then
  printf '| — | — | unknown | — | — | — | %s | freshness query unavailable |\n' "$grey"
  raise_unknown
else
  while IFS=$'\t' read -r source device last_event age events24 events7; do
    [[ -n "$device" ]] || continue
    excluded=0
    for excluded_id in "${EXCLUDED_DEVICE_IDS[@]}"; do [[ "$device" == "$excluded_id" ]] && excluded=1; done
    age_display=$age
    if [[ "$age" =~ ^[0-9]+$ ]]; then
      if [[ "$source" == Shelly ]]; then
        age_display="$((age / 60))m $((age % 60))s"
      else age_display="${age}h"
      fi
    fi
    if ((excluded)); then
      printf '| %s | %s | %s | %s | %s | %s | %s | %s |\n' "$source" "$device" "$last_event" "$age_display" "$events24" "$events7" "⚪ Excluded" "$EXCLUDED_DEVICE_REASON"
      continue
    fi
    fs=$grey; reason='missing measurement evidence'
    if [[ "$source" == Shelly ]]; then
      fs=$(shelly_freshness_status "$age")
      case "$fs" in
        "$green") reason='compact storage; last event <=5m' ;;
        "$yellow") reason='compact storage; last event >5m and <=15m'; raise_status 1 ;;
        "$red") reason='compact storage; last event >15m'; raise_status 2 ;;
        *) reason='compact storage; missing or invalid timestamp'; raise_unknown ;;
      esac
    elif [[ "$age" =~ ^[0-9]+$ ]]; then
      if (( age <= 24 )); then fs=$green; reason='within 24h freshness window'
      elif (( age <= 48 )); then fs=$yellow; reason='last event 24-48h ago'; raise_status 1
      else fs=$red; reason='last event older than 48h'; raise_status 2
      fi
    else
      raise_unknown
    fi
    printf '| %s | %s | %s | %s | %s | %s | %s | %s |\n' "$source" "$device" "$last_event" "$age_display" "$events24" "$events7" "$fs" "$reason"
  done <<< "$freshness_rows"
fi
printf '\n## 7. Systemd timers and scheduled jobs\n\n| Unit | Type | Enabled | Active | Result | Last run | Next run | Status |\n|---|---|---|---|---|---|---|---|\n'; for unit in "${EXPECTED_TIMERS[@]}";do en=$(timeout 5s systemctl is-enabled "$unit" 2>/dev/null||printf unknown);ac=$(timeout 5s systemctl is-active "$unit" 2>/dev/null||printf unknown);line=$(timeout 5s systemctl list-timers "$unit" --all --no-legend --no-pager 2>/dev/null|head -n1);service=$(awk '{print $NF}'<<<"$line");result=$(timeout 5s systemctl show "$service" -p Result --value 2>/dev/null||printf unknown);last=$(awk '{print $7" "$8" "$9}'<<<"$line");next=$(awk '{print $2" "$3" "$4}'<<<"$line");ts=$green;[[ "$en" == unknown||"$ac" == unknown||"$result" == unknown||-z "$line" ]]&&{ ts=$grey; raise_unknown; };[[ "$en" != enabled||"$ac" != active||"$result" != success ]]&&ts=$red;printf '| %s | timer | %s | %s | %s | %s | %s | %s |\n' "$unit" "$en" "$ac" "${result:-unknown}" "${last:-unknown}" "${next:-unknown}" "$ts";[[ "$ts" == "$red" ]]&&raise_status 2;done
printf '\n## 8. Application/API availability\n\n| Check | Path | Transport | HTTP status | Latency | Contract | TLS | Status |\n|---|---|---|---:|---:|---|---|---|\n'; code=unknown;time=unknown;api_status=$grey;if command_exists curl;then read -r code time<<<"$(curl -sS -o /dev/null -w '%{http_code} %{time_total}' --max-time 5 http://127.0.0.1:8000/health 2>/dev/null||printf '000 unknown')";[[ "$code" == 200 ]]&&api_status=$green||api_status=$red;fi;printf '| API local health | /health | loopback HTTP | %s | %ss | read-only GET | n/a | %s |\n' "$code" "$time" "$api_status";[[ "$api_status" == "$red" ]]&&raise_status 2
public_origin=${PUBLIC_API_ORIGIN:-https://telemetry.schoolheroz.com}; public_code=unknown; public_time=unknown; public_status=$grey; tls_status=$grey
if command_exists curl; then read -r public_code public_time <<<"$(timeout 15s curl -sS -o /dev/null -w '%{http_code} %{time_total}' --max-time 10 "$public_origin/internal/data/health" 2>/dev/null || printf '000 unknown')"; [[ "$public_code" == 200 || "$public_code" == 401 ]] && public_status=$green || public_status=$red; if [[ "$public_status" == "$green" ]]; then awk -v t="$public_time" 'BEGIN{exit !(t>3)}' && { public_status=$red; raise_status 2; } || { awk -v t="$public_time" 'BEGIN{exit !(t>=1)}' && { public_status=$yellow; raise_status 1; }; }; tls_status=$green; else tls_status=$red; fi; fi
printf '| Public API health | /internal/data/health | HTTPS/Caddy | %s | %ss | 200 or expected 401 auth boundary | %s | %s |\n' "$public_code" "$public_time" "$tls_status" "$public_status"; [[ "$public_status" == "$red" ]] && raise_status 2
printf '\n## 9. Scheduler inventory and evidence\n\n| Scheduler source | Entry / unit | Schedule / evidence | Enabled | Active | Result | Status |\n|---|---|---|---|---|---|---|\n'
cron_active=$(timeout 5s systemctl is-active cron 2>/dev/null || printf unknown); cron_enabled=$(timeout 5s systemctl is-enabled cron 2>/dev/null || printf unknown); cron_status=$green; [[ "$cron_active" == unknown || "$cron_enabled" == unknown ]]&&{ cron_status=$grey; raise_unknown; }; [[ "$cron_active" != active || "$cron_enabled" != enabled ]]&&{ cron_status=$red; raise_status 2; }; printf '| cron service | cron | — | %s | %s | — | %s |\n' "$cron_enabled" "$cron_active" "$cron_status"
root_entries=$(timeout 5s crontab -l 2>/dev/null | awk '!/^[[:space:]]*(#|$)/ && $1 !~ /^[A-Za-z_][A-Za-z0-9_]*=/{n++} END{print n+0}' || printf unknown); [[ "$root_entries" == unknown ]]&&raise_unknown; printf '| root crontab | root | %s active entries | yes | %s | inventory only | %s |\n' "$root_entries" "$cron_active" "$([[ "$root_entries" == unknown ]]&&printf "$grey"||printf "$green")"
for user_cron in /var/spool/cron/crontabs/*; do [[ -f "$user_cron" ]]||continue; user_entries=$(awk '!/^[[:space:]]*(#|$)/ && $1 !~ /^[A-Za-z_][A-Za-z0-9_]*=/{n++} END{print n+0}' "$user_cron" 2>/dev/null || printf unknown); printf '| user crontab | %s | %s active entries | yes | %s | inventory only | %s |\n' "$(basename "$user_cron")" "$user_entries" "$cron_active" "$([[ "$user_entries" == unknown ]]&&printf "$grey"||printf "$green")"; [[ "$user_entries" == unknown ]]&&raise_unknown; done
etc_entries=$(awk '!/^[[:space:]]*(#|$)/ && $1 !~ /^[A-Za-z_][A-Za-z0-9_]*=/{n++} END{print n+0}' /etc/crontab 2>/dev/null || printf unknown); [[ "$etc_entries" == unknown ]]&&raise_unknown; printf '| /etc/crontab | system | %s active entries | yes | %s | inventory only | %s |\n' "$etc_entries" "$cron_active" "$([[ "$etc_entries" == unknown ]]&&printf "$grey"||printf "$green")"
for cron_file in /etc/cron.d/*; do [[ -f "$cron_file" ]]||continue; entries=$(awk '!/^[[:space:]]*(#|$)/ && $1 !~ /^[A-Za-z_][A-Za-z0-9_]*=/{n++} END{print n+0}' "$cron_file" 2>/dev/null || printf unknown); printf '| /etc/cron.d | %s | %s active entries | yes | %s | inventory only | %s |\n' "$(basename "$cron_file")" "$entries" "$cron_active" "$([[ "$entries" == unknown ]]&&printf "$grey"||printf "$green")"; [[ "$entries" == unknown ]]&&raise_unknown; done
failed_units=$(timeout 5s systemctl --failed --no-legend 2>/dev/null | awk 'NF{n++} END{print n+0}' || printf unknown); scheduler_status=$green; [[ "$failed_units" == unknown ]]&&{ scheduler_status=$grey; raise_unknown; }; [[ "$failed_units" =~ ^[1-9] ]]&&{ scheduler_status=$red; raise_status 2; }; printf '| systemd failed units | scheduler scope | %s failed units | — | — | failure inventory | %s |\n' "$failed_units" "$scheduler_status"
printf '\n## 10. Weather / D+1 forecast coverage\n\n'
printf 'Athens civil-hour contract: 24 unique hours per date. PV tomorrow is due at 23:30 Athens (23:00 job + 30m grace); current-day coverage remains required. Weather refresh due at 23:20 (22:50 + 30m grace).\n\n'
printf '| Check | Target date | Persisted update / run (UTC) | Valid hours | Expected | Status | Evidence |\n|---|---|---|---:|---:|---|---|\n'
forecast_sql=$(cat <<'SQL'
WITH clock AS (
 SELECT now() AT TIME ZONE 'Europe/Athens' AS local_now
), dates AS (
 SELECT local_now::date AS today, local_now,
 (CASE WHEN local_now::time >= time '23:20' THEN local_now::date ELSE local_now::date-1 END + time '22:50') AT TIME ZONE 'Europe/Athens' AS weather_due
 FROM clock
), targets AS (
 SELECT today AS day FROM dates UNION ALL SELECT today+1 FROM dates
), weather AS (
 SELECT t.day,count(w.id) AS rows,
 count(DISTINCT w.forecast_timestamp) FILTER (WHERE w.forecast_date=t.day AND w.forecast_hour=extract(hour FROM w.forecast_timestamp) AND w.temperature_2m IS NOT NULL AND w.shortwave_radiation IS NOT NULL) AS valid,
 min(w.fetched_at) AS oldest, max(w.fetched_at) AS newest
 FROM targets t LEFT JOIN weather_hourly_forecasts w
 ON w.source='open-meteo' AND w.latitude=37.068 AND w.longitude=22.026 AND w.timezone='Europe/Athens'
 AND w.forecast_timestamp IN (SELECT generate_series(t.day::timestamp,t.day+time '23:00',interval '1 hour'))
 GROUP BY t.day
), pv AS (
 SELECT t.day,r.id,r.success,r.completed_at,count(h.id) AS rows,
 count(DISTINCT h.forecast_timestamp) FILTER (WHERE h.forecast_date=t.day AND h.forecast_hour=extract(hour FROM h.forecast_timestamp) AND h.predicted_power_kw IS NOT NULL AND h.predicted_power_kw>=0 AND h.forecast_timestamp IN (SELECT generate_series(t.day::timestamp,t.day+time '23:00',interval '1 hour'))) AS valid
 FROM targets t LEFT JOIN LATERAL (SELECT * FROM pv_day_ahead_forecast_runs WHERE forecast_date=t.day ORDER BY started_at DESC,id DESC LIMIT 1) r ON true
 LEFT JOIN pv_day_ahead_forecast_hourly h ON h.run_id=r.id
 GROUP BY t.day,r.id,r.success,r.completed_at
)
SELECT 'Weather',w.day,coalesce(to_char(w.newest AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'),'unknown'),w.valid,24,
 CASE WHEN w.rows<>24 OR w.valid<>24 THEN 'attention' WHEN w.oldest<d.weather_due THEN 'warning' ELSE 'healthy' END,
 'Open-Meteo 37.068/22.026; all hours must be from latest due refresh'
 FROM weather w CROSS JOIN dates d
UNION ALL
SELECT 'PV D+1',p.day,coalesce('run '||p.id||' / '||to_char(p.completed_at AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'),'unknown'),p.valid,24,
 CASE WHEN p.id IS NULL AND p.day=d.today+1 AND d.local_now::time<time '23:30' THEN 'pending'
 WHEN p.id IS NULL OR p.success IS NOT TRUE OR p.completed_at IS NULL OR p.rows<>24 OR p.valid<>24 THEN 'attention' ELSE 'healthy' END,
 CASE WHEN p.id IS NULL AND p.day=d.today+1 AND d.local_now::time<time '23:30' THEN 'Not due yet; expected after nightly run' ELSE 'Latest target-date run; success and exact hourly coverage required' END
 FROM pv p CROSS JOIN dates d ORDER BY 1,2;
SQL
)
if forecast_rows=$(timeout 20s docker exec -e PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=15000 -c lock_timeout=2000' "$DB_CONTAINER" psql -X -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME" -At -F '|' -c "$forecast_sql" 2>/dev/null) && [[ -n "$forecast_rows" ]]; then
 while IFS='|' read -r check target persisted valid expected result evidence; do
  case "$result" in
   healthy) forecast_status=$green ;;
   warning) forecast_status=$yellow; raise_status 1 ;;
   attention) forecast_status=$red; raise_status 2 ;;
   pending) forecast_status='⚪ Not due yet' ;;
   *) forecast_status=$grey; raise_unknown ;;
  esac
  printf '| %s | %s | %s | %s | %s | %s | %s |\n' "$check" "$target" "$persisted" "$valid" "$expected" "$forecast_status" "$evidence"
 done <<< "$forecast_rows"
else
 printf '| Weather / PV D+1 | unknown | unknown | — | 24 | %s | Query failed, timed out or returned no evidence |\n' "$grey"
 raise_unknown
fi
printf '\n## 11. Simulation results by active school\n\n'
printf 'Athens civil-hour contract: six supported schools; nightly simulation is due at 23:40 Athens (23:10 job + 30m grace).\n\n'
printf '| School | Target date | Latest run | Completed | Requested rooms | Successful rooms | Failed rooms | Status | Reason |\n|---|---|---|---|---:|---:|---:|---|---|\n'
simulation_sql=$(cat <<'SQL'
WITH clock AS (SELECT now() AT TIME ZONE 'Europe/Athens' AS local_now), schools(school_id) AS (
 VALUES ('school_3'),('school_7'),('school_10'),('school_13'),('school_22'),('school_23')
), latest AS (
 SELECT s.school_id,c.local_now,
        r.id,r.day_ahead_date,r.success,r.completed_at,r.requested_rooms,r.successful_rooms,r.failed_rooms
 FROM schools s CROSS JOIN clock c
 LEFT JOIN LATERAL (
   SELECT * FROM simulation_day_ahead_runs r
   WHERE r.school_id=s.school_id AND r.day_ahead_date=c.local_now::date
   ORDER BY r.started_at DESC,r.id DESC LIMIT 1
 ) r ON true
)
SELECT school_id,local_now::date,coalesce(id::text,'unknown'),coalesce(to_char(completed_at AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'),'unknown'),
       coalesce(requested_rooms::text,'—'),coalesce(successful_rooms::text,'—'),coalesce(failed_rooms::text,'—'),
       CASE WHEN id IS NULL AND local_now::time<time '23:40' THEN 'pending'
            WHEN id IS NULL OR success IS NOT TRUE OR completed_at IS NULL OR coalesce(failed_rooms,0)<>0 THEN 'attention'
            ELSE 'healthy' END,
       CASE WHEN id IS NULL AND local_now::time<time '23:40' THEN 'Not due yet; expected after nightly run'
            WHEN id IS NULL THEN 'No persisted run for today'
            WHEN success IS NOT TRUE THEN 'Persisted run is unsuccessful'
            WHEN completed_at IS NULL THEN 'Run has no completion evidence'
            WHEN coalesce(failed_rooms,0)<>0 THEN 'One or more room results failed'
            ELSE 'Persisted successful run with no failed rooms' END
FROM latest ORDER BY school_id;
SQL
)
if simulation_rows=$(timeout 20s docker exec -e PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=15000 -c lock_timeout=2000' "$DB_CONTAINER" psql -X -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME" -At -F '|' -c "$simulation_sql" 2>/dev/null) && [[ -n "$simulation_rows" ]]; then
 while IFS='|' read -r school target run completed requested successful failed result reason; do
  case "$result" in
   healthy) simulation_status=$green ;;
   attention) simulation_status=$red; raise_status 2 ;;
   pending) simulation_status='⚪ Not due yet' ;;
   *) simulation_status=$grey; raise_unknown ;;
  esac
  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' "$school" "$target" "$run" "$completed" "$requested" "$successful" "$failed" "$simulation_status" "$reason"
 done <<< "$simulation_rows"
else
 printf '| — | unknown | unknown | unknown | — | — | — | %s | Query failed, timed out or returned no evidence |\n' "$grey"
 raise_unknown
fi
exit_code=$overall; ((has_unknown && overall==0)) && exit_code=3
printf '\nOverall status: ';((overall>=2))&&printf '%s\n' "$red"||{ ((overall==1))&&printf '%s\n' "$yellow"||{ ((has_unknown))&&printf '%s\n' "$grey"||printf '%s\n' "$green"; };};exit "$exit_code"
