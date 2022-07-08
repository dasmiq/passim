@echo off

:: Replace special characters <, >, |, & in all arguments with a placeholder
:: which will be replaced with the original character in seriatim.py
:: NB: unfortunately, this does not work with the equals sign; use _EQ_, _LTE_ and _GTE_ instead.

:: make sure variables are not expanded before the script is run:
SETLOCAL EnableDelayedExpansion

:: first, set up an empty variable that will contain all modified variables:
set modifiedArgs=

:: loop through all arguments passed to the script (%*)
:: and replace all special characters in them
:: finally, add each argument to the modified arguments list
for %%i in (%*) do (
  set arg=%%i
  set arg=!arg:^<=_LT_!
  set arg=!arg:^>=_GT_!
  set arg=!arg:^|=_PIPE_!
  set arg=!arg:^&=_AMPERSAND_!
  set modifiedArgs=!modifiedArgs! !arg!
  echo !arg!
)
::echo MODIFIED ARGS:
::echo %modifiedArgs%

echo %~dp0seriatim.cmd --all-pairs --fields "xxhash64(series) as gid" -f gid_LT_gid2 %modifiedArgs%
call %~dp0seriatim.cmd --all-pairs --fields "xxhash64(series) as gid" -f gid_LT_gid2 %modifiedArgs%
