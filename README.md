# Average Temperature — MapReduce

Big Data (PUCPR) activity: the average temperature of `forestfireinput.csv` computed with Hadoop
MapReduce.

## How it works

The input has no header; the temperature is column 8 (0-based). Every reading goes under the
same global key with a composite value `(sum, count)`, and the division happens only once, in the
reducer. Averaging partial averages would be wrong whenever the parts hold different numbers of
readings (Lecture 05).

```
Map:     "7,5,mar,fri,...,8.2,..."  ->  ("temperature", (8.2, 1))
Shuffle: group by key               ->  ("temperature", [(8.2, 1), (18.0, 1), ...])
Reduce:  sum(temp) / sum(count)     ->  ("temperature", 18.889168278529986)
```

| Class | Role |
| --- | --- |
| `AverageTemperature` | Driver (`main`): configures and submits the job |
| `TemperatureMapper` | Map: parses a line, emits `("temperature", (temp, 1))` |
| `TemperatureReducer` | Reduce: sums values and counts, emits the average |
| `SumCountWritable` | Custom `Writable` for the composite value `(sum, count)` |

Result: **18.889168278529986** over 517 readings (sum 9765.7).

## Running

Requirements: Maven and **Java 8, 11 or 17**. Hadoop 3.3.6 does not run on Java 23 or later.

```bash
mvn package                                   # target/average-temperature-1.0.0.jar

# On a machine/VM with Hadoop installed (output dir must not exist):
hdfs dfs -put input/forestfireinput.csv /forestfire/input.csv
hadoop jar target/average-temperature-1.0.0.jar /forestfire/input.csv /forestfire/output
hdfs dfs -cat /forestfire/output/part-r-00000

# Hadoop local mode, no cluster (Linux/macOS; on Windows Hadoop also needs winutils.exe):
mvn compile exec:java                         # reads input/, writes target/output/
cat target/output/part-r-00000
```
