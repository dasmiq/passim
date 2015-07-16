#!/usr/bin/env bash

jq -c '.members[] as $m | [{cluster: .id, size: .size}, $m] | add'
