# 🧾 **Instructions to Run Stripe Co-occurrence**

## 1. Required Files

Make sure the following files are present:

### Code

* `stripe_50.java`

### Supporting Files

* `stopwords.txt`
* `words.txt`

### Dataset

* Folder: `Wikipedia-EN-20120601_ARTICLES` 

---

## Expected Directory Structure

```text
project/
│── stripe_50.java
│── stopwords.txt
│── words.txt
│── Wikipedia-EN-20120601_ARTICLES/
```

---

# ⚙️ 2. Start Hadoop & Upload Data

```bash
start-dfs.sh

hdfs dfs -mkdir -p /user/$USER/input

# Upload dataset
hdfs dfs -put ../Wikipedia-EN-20120601_ARTICLES /user/$USER/input/

# Upload required files
hdfs dfs -put stopwords.txt /user/$USER/
hdfs dfs -put words.txt /user/$USER/
```

---

# 3. Merge Dataset 


```bash
hdfs dfs -getmerge /user/$USER/input/Wikipedia-EN-20120601_ARTICLES /tmp/merged_input.txt
```

Re-upload merged file:

```bash
hdfs dfs -rm -r /user/$USER/input_merged 2>/dev/null
hdfs dfs -mkdir -p /user/$USER/input_merged
hdfs dfs -put /tmp/merged_input.txt /user/$USER/input_merged/
```

---

# 4. Compile & Create JAR

```bash
javac -classpath `hadoop classpath` -d build stripe_50.java
jar -cvf stripe.jar -C build/ .
```

---

# 5. Run the Job

```bash
sleep 5
hdfs dfsadmin -safemode leave

for d in 1 2 3 4; do
    hdfs dfs -rm -r /user/$USER/output_d${d} 2>/dev/null

    echo "===== d = ${d} =====" >> runtimes.txt

    { time HADOOP_OPTS="-Xmx4g" hadoop jar stripe.jar stripe_50 \
        /user/$USER/input_merged \
        /user/$USER/output_d${d} \
        /user/$USER/stopwords.txt \
        ${d} \
        /user/$USER/words.txt; } 2> time_tmp.txt

    cat time_tmp.txt
    grep -E "^real|^user|^sys" time_tmp.txt >> runtimes.txt
    echo "" >> runtimes.txt
done

rm time_tmp.txt
```

---

# 6. Download Outputs

```bash
mkdir -p outputs

for d in 1 2 3 4; do
    hdfs dfs -get /user/$USER/output_d${d}/part-r-00000 outputs/output_d${d}.txt
done
```

---

# 7. Cleanup

```bash
rm /tmp/merged_input.txt
stop-dfs.sh
```

---
