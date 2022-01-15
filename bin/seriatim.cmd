@echo off

echo %*
echo %1
echo %2
echo %3
echo %4
echo %5
echo %6
echo %7

set WRAPPED=%~dp0..\share\submit-seriatim.py

set RAW=%~dp0..\passim\seriatim.py

if exist $RAW (
    set SCRIPT=%RAW%
) else (
    set SCRIPT=%WRAPPED%
)

set SPARK_SUBMIT_ARGS=%SPARK_SUBMIT_ARGS%

echo spark-submit --repositories https://repos.spark-packages.org/ --packages graphframes:graphframes:0.8.0-spark3.0-s_2.12 %SPARK_SUBMIT_ARGS% %SCRIPT% %*

spark-submit --repositories https://repos.spark-packages.org/ ^
  --packages graphframes:graphframes:0.8.0-spark3.0-s_2.12 ^
    %SPARK_SUBMIT_ARGS% %SCRIPT% %*
