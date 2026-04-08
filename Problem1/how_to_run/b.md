# 📘 HOW TO RUN — Problem 1 (Part B)

---

## 🧠 Overview

This program computes the **co-occurrence matrix** of words using the **pairs approach**.

* Uses top 50 frequent words from Part (a)
* Computes word pairs within a distance **d**
* Runs for **d = 1, 2, 3, 4**
* Outputs frequency of co-occurring pairs

---

## ⚙️ Prerequisites

* Hadoop running
* Part (a) completed
* `a.txt` (Top 50 words) available

---

## 🚀 STEP 1 — Upload Top 50 Words to HDFS

```bash
hdfs dfs -put output_local/a.txt /top50.txt
```

---

## 🔨 STEP 2 — Compile

```bash
javac --release 11 -classpath "$(hadoop classpath)" -d build src/CoOccurrence.java
```

---

## 📦 STEP 3 — Create JAR

```bash
cd build
jar cf CoOccurrence.jar *.class
cd ..
```

---

## 🧹 STEP 4 — Run for different distances

---

### ▶️ d = 1

```bash
hdfs dfs -rm -r /output_b_d1
hadoop jar build/CoOccurrence.jar CoOccurrence /wiki /output_b_d1 /top50.txt 1
```

---

### ▶️ d = 2

```bash
hdfs dfs -rm -r /output_b_d2
hadoop jar build/CoOccurrence.jar CoOccurrence /wiki /output_b_d2 /top50.txt 2
```

---

### ▶️ d = 3

```bash
hdfs dfs -rm -r /output_b_d3
hadoop jar build/CoOccurrence.jar CoOccurrence /wiki /output_b_d3 /top50.txt 3
```

---

### ▶️ d = 4

```bash
hdfs dfs -rm -r /output_b_d4
hadoop jar build/CoOccurrence.jar CoOccurrence /wiki /output_b_d4 /top50.txt 4
```

---

## 📊 STEP 5 — Save Output to Local Files

```bash
hdfs dfs -cat /output_b_d1/part-r-00000 > output_local/1b_d1.txt
hdfs dfs -cat /output_b_d2/part-r-00000 > output_local/1b_d2.txt
hdfs dfs -cat /output_b_d3/part-r-00000 > output_local/1b_d3.txt
hdfs dfs -cat /output_b_d4/part-r-00000 > output_local/1b_d4.txt
```

---

## 📁 Final Output Files

```text
output_local/
 ├── 1b_d1.txt
 ├── 1b_d2.txt
 ├── 1b_d3.txt
 └── 1b_d4.txt
```

---

## 🧠 Output Format

Each line represents a co-occurring pair:

```text
word1,word2    frequency
```

Example:

```text
data,science    15
science,data    15
```

---

## 🧠 Notes

* Only top 50 words from Part (a) are considered
* Distance `d` controls how far apart words can be
* Larger `d` → more pairs → higher counts
* Results are based on **pairs approach**

---

## 🎯 End of Execution

---
