# 📘 HOW TO RUN — Problem 1 (Part A)

---

## 🧠 Overview

This program performs:

* Word count using Hadoop MapReduce
* Removes stopwords using Distributed Cache
* Filters only alphabetic words
* Outputs Top 50 most frequent words

---

## ⚙️ Prerequisites

* Hadoop installed and configured
* Java 11 installed
* SSH service available

---

## 🚀 STEP 1 — Start Hadoop

```bash
sudo service ssh start
source ~/.bashrc
cd ~/hadoop_setup
start-dfs.sh
```

### Verify:

```bash
jps
```

Expected:

```
NameNode
DataNode
SecondaryNameNode
```

---

## 📂 STEP 2 — Go to project directory



## 📦 STEP 3 — Upload data to HDFS

```bash
hdfs dfs -rm -r /wiki
hdfs dfs -mkdir /wiki
hdfs dfs -put input/Wikipedia-50-ARTICLES/* /wiki
hdfs dfs -put input/stopwords.txt /wiki
```

---

## 🔨 STEP 4 — Compile (Java 11)

```bash
javac --release 11 -classpath "$(hadoop classpath)" -d build src/TopWords.java
```

---

## 📦 STEP 5 — Create JAR

```bash
cd build
jar cf TopWords.jar *.class
```

---

## 🧹 STEP 6 — Remove previous output

```bash
hdfs dfs -rm -r /output_a
```

(ignore error if directory does not exist)

---

## ▶️ STEP 7 — Run MapReduce Job

```bash
hadoop jar TopWords.jar TopWords /wiki /output_a /wiki/stopwords.txt
```

---

## 📊 STEP 8 — Get Top 50 words

```bash
hdfs dfs -cat /output_a/part-r-00000 | sort -k2 -nr | head -50 > ../how_to_run/a.txt
```

---

## 📁 Final Output

Top 50 words are saved in:

```
output_local/a.txt
```

---

## 🧠 Notes

* Stopwords are removed using Distributed Cache
* Only alphabetic words are considered
* Hadoop does not overwrite output directories
* Always delete `/output_a` before running

---

## 🎯 End of Execution

---
