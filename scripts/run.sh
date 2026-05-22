#!/usr/bin/env bash
# Run DS200 Lab04 tasks 1-5 + 6,7,9 with Java Spark DataFrame in local mode.
# Prerequisites: Java 11+, Maven, and Spark (spark-submit on PATH).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SPARK_PROJ="$ROOT/main"
JAR="$SPARK_PROJ/target/lab04-dataframe-1.0.0.jar"
DATA="$ROOT/data"
OUT="$ROOT/output"

for cmd in spark-submit mvn; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing '$cmd' on PATH. Install Apache Spark and Maven, then retry." >&2
    exit 1
  fi
done

mkdir -p "$OUT"

echo "Building JAR..."
(
  cd "$SPARK_PROJ"
  mvn -q package -DskipTests
)
echo "Build complete: $JAR"
echo ""

echo "Task 1 - Load CSV files with inferSchema..."
spark-submit --master local[*] --class task1.Task1App "$JAR" \
  "$DATA" "$OUT/task1.txt"

echo "Task 2 - Total orders, customers, and sellers..."
spark-submit --master local[*] --class task2.Task2App "$JAR" \
  "$DATA" "$OUT/task2.txt"

echo "Task 3 - Orders by country (descending)..."
spark-submit --master local[*] --class task3.Task3App "$JAR" \
  "$DATA" "$OUT/task3.txt"

echo "Task 4 - Orders by year/month..."
spark-submit --master local[*] --class task4.Task4App "$JAR" \
  "$DATA" "$OUT/task4.txt"

echo "Task 5 - Review score statistics..."
spark-submit --master local[*] --class task5.Task5App "$JAR" \
  "$DATA" "$OUT/task5.txt"

echo "Task 6 - Revenue 2024 by product category..."
spark-submit --master local[*] --class task6.Task6App "$JAR" \
  "$DATA" "$OUT/task6.txt"

echo "Task 7 - Top-selling products + avg review score..."
spark-submit --master local[*] --class task7.Task7App "$JAR" \
  "$DATA" "$OUT/task7.txt"

echo "Task 9 - Customer segmentation..."
spark-submit --master local[*] --class task9.Task9App "$JAR" \
  "$DATA" "$OUT/task9.txt"

echo ""
echo "All tasks complete. Reports written to: $OUT"
