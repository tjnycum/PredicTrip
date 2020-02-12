#!/usr/bin/env bash
# Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

# NOTE: these functions assume all (and only) files you want to include have .csv extension

function bytes_in_first_line () { head -n 1 "$1" | wc -c; }

function max_of_first_line_bytes () {
  declare -a byte_counts
  for file in *.csv; do
    # append new value to byte_counts array. without outer parentheses would increment first element instead
    byte_counts+=($(bytes_in_first_line "$file"))
  done
  # use local to avoid altering IFS in outer env
  local IFS=$'\n'
  echo "${byte_counts[*]}" | sort -n -r | head -n 1
}
