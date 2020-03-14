#!/usr/bin/env bash
# Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

function clear_logs () {
  # TODO: make this get log locations from config files
  for arg in "${@}"; do
    case "$arg" in
      hadoop)
        rm /var/log/hadoop/*
        ;;
      hbase)
        rm /var/log/hbase/*
        ;;
      geomesa)
        rm /var/log/geomesa-tools/*
        ;;
      *)
        # TODO: display usage message listing valid arguments
        echo "unrecognized argument: $arg"
        val=1
        ;;
    esac
  done
  return ${val:-0}
}
