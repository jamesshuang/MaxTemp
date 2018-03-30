# MaxTemp - Hadoop MapReduce

Finds the mean maximum temperature for everyday of the year for each weather station using MapReduce.

To compile:
1. `hadoop com.sun.tools.javac.Main MaxTemp.java`
2. `jar cf mt.jar MaxTemp*.class`

To run:
1. `hadoop jar mt.jar MaxTemp <input path> <output path for first job> <final output path>`

Note: This program runs 2 MapReduce jobs to get the final output, so you will need two output paths
- the output directory for the first job is the input directory for the second job
- the output directory for the second job is final output directory, which contains the mean max temps

The first output directory is job1out

The second output directory is job2out
- this directory contains the mean max temps for everyday of the year for every weather station
