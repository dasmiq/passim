@echo off
call %~dp0seriatim --all-pairs --fields "xxhash64(series) as gid" -f gidLESSTHANgid2 %*
