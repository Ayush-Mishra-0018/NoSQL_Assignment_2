# 1. Clean HDFS and Local output
hdfs dfs -rm -r /output_a_big
rm -f output_local/a.txt

# 2. Compile and Jar
javac --release 11 -classpath "$(hadoop classpath)" -d build src/TopWordsOnBigData.java
jar cf build/TopWordsOnBigData.jar -C build .

# 3. Execute with Runtime Tracking
time hadoop jar build/TopWordsOnBigData.jar TopWordsOnBigData /wiki /output_a_big /wiki/stopwords.txt

# 4. Save and Verify Readable Results
hadoop fs -getmerge /output_a_big output_local/a.txt
cat output_local/a.txt