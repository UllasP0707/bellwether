#!/usr/bin/env bash
# Run the daily DAG for one date twice and prove nothing moved.
#
# This is the claim every backfill story rests on and the one nobody ever
# checks: reprocessing a day has to produce what processing it once produced.
# It is a property of the tasks, not of the orchestrator — every stage here
# replaces its day rather than appending, so it holds whether Airflow runs it,
# a human runs it, or a retry runs it twice by accident.
#
#   ./scripts/backfill_twice.sh 2026-08-25
set -euo pipefail

DATE="${1:-2026-08-25}"
compose="docker compose"
psql="$compose exec -T postgres psql -U bellwether -tAc"

snapshot() {
  $psql "
    SELECT 'raw_daily_employee_signal='||count(*) FROM raw_daily_employee_signal
    UNION ALL SELECT 'raw_daily_population_signal='||count(*) FROM raw_daily_population_signal
    UNION ALL SELECT 'raw_employee_score='||count(*)  FROM raw_employee_score
    UNION ALL SELECT 'fct_employee_daily_risk='||count(*) FROM analytics_marts.fct_employee_daily_risk
    UNION ALL SELECT 'mart_department_risk='||count(*)    FROM analytics_marts.mart_department_risk
    UNION ALL SELECT 'mart_cohort_risk='||count(*)        FROM analytics_marts.mart_cohort_risk
  " | tr -d ' '
}

run_dag() {
  $compose --profile orchestration run --rm airflow \
    "airflow db migrate >/dev/null 2>&1; airflow dags test bellwether_daily $DATE >/tmp/dag.log 2>&1;
     printf 'run exit=%s, tasks succeeded=%s\n' \"\$?\" \"\$(grep -ac 'Marking task as SUCCESS' /tmp/dag.log)\""
}

echo "run 1 for $DATE"
run_dag
before=$(snapshot)

echo "run 2 for $DATE"
run_dag
after=$(snapshot)

echo
echo "row counts after each run:"
paste <(echo "$before") <(echo "$after") | awk -F'\t' '{printf "  %-38s %s\n", $1, ($1 == $2 ? "unchanged" : "CHANGED -> " $2)}'

if [ "$before" = "$after" ]; then
  echo
  echo "identical. reprocessing a day is a no-op."
else
  echo
  echo "the backfill is not idempotent" >&2
  exit 1
fi
