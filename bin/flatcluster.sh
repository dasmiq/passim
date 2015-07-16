#!/usr/bin/env bash

jq -c '.members[] as $m | [{cluster: .id, size: .size, series: ($m.series // $m.name), begin: ($m.begin // $m.start)}, ($m | del(.name, .start))] | add' "$@"
