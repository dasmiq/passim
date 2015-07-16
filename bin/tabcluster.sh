#!/usr/bin/env bash

jq -r '[.cluster, .size, .date, .series, .title, .url, .begin, .end, (.text|@json)] | map(tostring) | join("\t")' "$@"
