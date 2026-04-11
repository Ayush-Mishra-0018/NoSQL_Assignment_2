# Problem 1 Part (b): Co-occurring Word Matrix (Pairs Approach)

This document contains the complete sequence of commands to generate the co-occurrence matrix and record the runtimes for distances d={1,2,3,4}.

# 1. HDFS Environment Reset
Ensure the Top 50 results from Part (a) are correctly placed in HDFS for the Distributed Cache.

```bash
# Clean up old output and metadata files
hdfs dfs -rm -r /output_b_d1 /output_b_d2 /output_b_d3 /output_b_d4 2>/dev/null
hdfs dfs -rm /top50.txt 2>/dev/null

# Upload the frequent words list (a.txt) from your local output_local folder to HDFS root
hdfs dfs -put output_local/a.txt /top50.txt

# Compile from the Problem1 root directory
javac --release 11 -classpath "$(hadoop classpath)" -d build src/CoOccurrence2.java

# Create the executable JAR file
cd build
jar cf CoOccurrence2.jar *.class
cd ..

Run this script block to execute all four experiments sequentially. It calculates the runtime for each job and saves it to a dedicated text file in output_local.

Bash

run_experiment() {
    d=$1
    out_path="/output_b_d${d}"
    rt_log="output_local/1b_d${d}_rt.txt"
    
    echo "------------------------------------------"
    echo "Starting MapReduce Job for distance d=${d}..."
    
    # Capture start time in seconds using the shell's built-in variable
    start_time=$SECONDS
    
    # Execute the Pairs algorithm
    # Args: <JarPath> <ClassName> <InputPath> <OutputPath> <Top50CachePath> <DistanceValue>
    hadoop jar build/CoOccurrence2.jar CoOccurrence2 /wiki $out_path /top50.txt $d
    
    # Calculate duration
    end_time=$SECONDS
    duration=$((end_time - start_time))
    
    # Save runtime and merge results into a single readable file
    echo "Distance d=${d} Runtime: ${duration} seconds" > "$rt_log"
    hadoop fs -getmerge $out_path "output_local/1b_d${d}.txt"
    
    echo "DONE: d=${d} completed in ${duration}s."
}

# Run the automated tasks for d=1, 2, 3, and 4
run_experiment 1
run_experiment 2
run_experiment 3
run_experiment 4