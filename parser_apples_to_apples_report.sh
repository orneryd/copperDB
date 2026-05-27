#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
copper_dir="$script_dir"
nornic_dir="$(cd "$script_dir/../NornicDB" && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

echo "Running CopperDB validation report..."
(
  cd "$copper_dir"
  cargo test --release -p copperdb-cypher parser_validation_benchmark_report -- --ignored --nocapture
) | tee "$tmp_dir/copper_validate.txt"

echo
echo "Running CopperDB shallow validation report..."
(
  cd "$copper_dir"
  cargo test --release -p copperdb-cypher parser_validation_shallow_benchmark_report -- --ignored --nocapture
) | tee "$tmp_dir/copper_validate_shallow.txt"

echo
echo "Running CopperDB parse report..."
(
  cd "$copper_dir"
  cargo test --release -p copperdb-cypher parser_benchmark_report -- --ignored --nocapture
) | tee "$tmp_dir/copper_parse.txt"

echo
echo "Running CopperDB stage split report..."
(
  cd "$copper_dir"
  cargo test --release -p copperdb-cypher parser_stage_benchmark_report -- --ignored --nocapture
) | tee "$tmp_dir/copper_stages.txt"

echo
echo "Running NornicDB validation and parse reports..."
(
  cd "$nornic_dir"
  NORNIC_RUN_PARSER_REPORTS=1 go test -run 'TestParser(ValidationBenchmarkReport|ParseBenchmarkReport)$' ./pkg/cypher -count=1 -v
) | tee "$tmp_dir/nornic_reports.txt"

awk '
function parse_summary(file,    line, parts, key, val, i) {
  while ((getline line < file) > 0) {
    if (line !~ /^PARSER_REPORT_SUMMARY /) {
      continue
    }
    split(line, parts, " ")
    mode = parser = total_ns = success = total_cases = ""
    for (i = 1; i <= length(parts); i++) {
      split(parts[i], kv, "=")
      key = kv[1]
      val = kv[2]
      if (key == "mode") mode = val
      else if (key == "parser") parser = val
      else if (key == "total_ns") total_ns = val
      else if (key == "success") success = val
      else if (key == "total_cases") total_cases = val
    }
    totals[mode ":" parser] = total_ns
    successes[mode ":" parser] = success "/" total_cases
  }
  close(file)
}

function fmt_us(ns) {
  return sprintf("%.3f us", ns / 1000.0)
}

BEGIN {
  parse_summary(ARGV[1])
  parse_summary(ARGV[2])
  parse_summary(ARGV[3])
  parse_summary(ARGV[4])
  parse_summary(ARGV[5])

  delete ARGV[1]
  delete ARGV[2]
  delete ARGV[3]
  delete ARGV[4]
  delete ARGV[5]

  print ""
  print "================================================================================"
  print "APPLES-TO-APPLES CYPHER PARSER REPORT"
  print "================================================================================"
  print "Method: same 38-query corpus, median of 5 runs per query class, split by stage."
  print "Stage validate: syntax/validation only."
  print "Stage parse: full standalone parse/AST or parse-tree construction, no execution."
  print ""
  printf "%-10s | %-10s | %-14s | %s\n", "Stage", "Parser", "Total", "Success"
  print "--------------------------------------------------------------------------------"
  printf "%-10s | %-10s | %-14s | %s\n", "validate", "CopperDB", fmt_us(totals["validate:CopperDB"]), successes["validate:CopperDB"]
  printf "%-10s | %-10s | %-14s | %s\n", "validate*", "CopperDB", fmt_us(totals["validate_shallow:CopperDB"]), successes["validate_shallow:CopperDB"]
  printf "%-10s | %-10s | %-14s | %s\n", "validate", "Nornic", fmt_us(totals["validate:Nornic"]), successes["validate:Nornic"]
  printf "%-10s | %-10s | %-14s | %s\n", "validate", "ANTLR", fmt_us(totals["validate:ANTLR"]), successes["validate:ANTLR"]
  printf "%-10s | %-10s | %-14s | %s\n", "parse", "CopperDB", fmt_us(totals["parse:CopperDB"]), successes["parse:CopperDB"]
  printf "%-10s | %-10s | %-14s | %s\n", "parse", "Nornic", fmt_us(totals["parse:Nornic"]), successes["parse:Nornic"]
  printf "%-10s | %-10s | %-14s | %s\n", "parse", "ANTLR", fmt_us(totals["parse:ANTLR"]), successes["parse:ANTLR"]
  print "--------------------------------------------------------------------------------"
  printf "Validation speedup: CopperDB vs ANTLR = %.2fx\n", totals["validate:ANTLR"] / totals["validate:CopperDB"]
  printf "Validation speedup: CopperDB shallow vs ANTLR = %.2fx\n", totals["validate:ANTLR"] / totals["validate_shallow:CopperDB"]
  printf "Validation speedup: Nornic vs ANTLR   = %.2fx\n", totals["validate:ANTLR"] / totals["validate:Nornic"]
  printf "Validation speedup: Nornic vs CopperDB = %.2fx\n", totals["validate:CopperDB"] / totals["validate:Nornic"]
  printf "Validation speedup: Nornic vs CopperDB shallow = %.2fx\n", totals["validate_shallow:CopperDB"] / totals["validate:Nornic"]
  print ""
  printf "Parse speedup: CopperDB vs ANTLR = %.2fx\n", totals["parse:ANTLR"] / totals["parse:CopperDB"]
  printf "Parse speedup: Nornic vs ANTLR   = %.2fx\n", totals["parse:ANTLR"] / totals["parse:Nornic"]
  printf "Parse speedup: CopperDB vs Nornic = %.2fx\n", totals["parse:Nornic"] / totals["parse:CopperDB"]
  print ""
  print "CopperDB internal stage split"
  printf "tokenize      | %-14s | %s\n", fmt_us(totals["tokenize:CopperDB"]), successes["tokenize:CopperDB"]
  printf "validate_shallow | %-12s | %s\n", fmt_us(totals["validate_shallow:CopperDB"]), successes["validate_shallow:CopperDB"]
  printf "validate_core | %-14s | %s\n", fmt_us(totals["validate_core:CopperDB"]), successes["validate_core:CopperDB"]
  printf "parse_core    | %-14s | %s\n", fmt_us(totals["parse_core:CopperDB"]), successes["parse_core:CopperDB"]
  print "================================================================================"
}
' "$tmp_dir/copper_validate.txt" "$tmp_dir/copper_validate_shallow.txt" "$tmp_dir/copper_parse.txt" "$tmp_dir/nornic_reports.txt" "$tmp_dir/copper_stages.txt"