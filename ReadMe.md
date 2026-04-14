# NoSQL Assignment 2 — MapReduce & Apache Hadoop

**Course:** DAS 839 - NoSQL Systems

**Team Members:**
- Harsh Sinha — IMT2023571
- Ayush Mishra — IMT2023129
- Santhosh Vodnala — IMT2023622
- Syed Naveed Mohammed — IMT2023119

---

## Overview

This project implements scalable text processing algorithms using Apache Hadoop MapReduce on a large Wikipedia corpus. The assignment covers co-occurrence matrix construction and TF-IDF computation across ~10,000 Wikipedia articles.

---

## Project Structure

```
NoSQL_Assignment_2/
├── ReadMe.md
├── instructions/
│   ├── problem_1a.txt
│   ├── problem_1b.txt
│   ├── problem_1c.txt
│   ├── problem_1e.txt
│   ├── problem_2a.txt
│   └── problem_2b.txt
├── jars/
│   ├── stripe.jar               # Problem 1c
│   ├── stripe_inmap.jar         # Problem 1e (stripe in-mapper)
│   ├── stripe_map.jar           # Problem 1e (stripe map)
│   ├── CoOccurence3.jar         # Problem 1e (pairs)
│   ├── CoOccurence2.jar         # Problem 1b
│   ├── TopWordsOnBigData.jar    # Problem 1a
│   ├── 2a.jar
│   └── assign2b.jar
├── problem1/
│   ├── a/
│   │   └── TopWordsOnBigData.java
│   ├── b/
│   │   └── CoOccurrence2.java
│   ├── c/
│   │   └── stripe_50.java
│   ├── e/
│   │   ├── CoOccurence3.java
│   │   ├── StripeInMapper.java
│   │   └── StripeMapFunction.java
│   └── outputs/
└── problem2/
    ├── problem2a/
    │   ├── df_full.tsv
    │   ├── df_top100_hdfs.tsv
    │   ├── stopwords.txt
    │   └── DFProject/
    └── question_2b/
        ├── code/
        │   └── TFIDF.java
        └── output/
            ├── output.tsv
            └── part-r-00000
```

> **Note:** JAR files to run are in `jars/`. Execution instructions are in `instructions/`.

---

## Dataset

**Wikipedia-EN-20120601_ARTICLES.tar.gz** — ~10,000 English Wikipedia articles, each treated as an individual document. Stored in HDFS for distributed processing.

A `stopwords.txt` file is used to filter common English words and is distributed to all mapper nodes via Hadoop's Distributed Cache.

---

## Problem 1 — Co-occurring Word Matrix

### Part (a): Top 50 Frequent Words

Identifies the top 50 most frequent words using the Pairs approach. Stopwords are filtered via Distributed Cache. A Combiner performs local aggregation to reduce shuffle overhead.

**Run:**
```bash
hadoop jar jars/TopWordsOnBigData.jar <input_path> <output_path> <stopwords_path>
```

**Sample Output:**
```
quot    267438
this    58446
first   41016
```

---

### Part (b): Co-occurrence Matrix — Pairs Approach

Builds a co-occurrence matrix for the top 50 words within a window distance `d`. The top 50 word list is loaded via Distributed Cache.

**Run:**
```bash
hadoop jar jars/CoOccurence2.jar <input_path> <output_path> <top50_path> <d>
```

**Sample Output:**
```
(city,york)     2172
(1,april)       722
```

**Runtime (seconds):**

| d | Runtime (s) |
|---|-------------|
| 1 | 387 |
| 2 | 372 |
| 3 | 394 |
| 4 | 387 |

---

### Part (c): Co-occurrence Matrix — Stripes Approach

Builds a co-occurrence matrix using stripes, reducing intermediate keys by grouping all neighbors of a word into a single associative array. Uses a Combiner for local stripe merging.

**Run:**
```bash
hadoop jar jars/stripe.jar <input_path> <output_path> <top50_path> <stopwords_path> <d>
```

**Sample Output:**
```
city    {york=2172, state=842, population=631}
new     {york=2172, jersey=945}
```

**Runtime (seconds):**

| d | Runtime (s) |
|---|-------------|
| 1 | 27.15 |
| 2 | 27.07 |
| 3 | 27.09 |
| 4 | 27.20 |

---

### Part (e): Local Aggregation Analysis

Compares four aggregation strategies:

| Strategy | d=1 (s) | d=2 (s) | d=3 (s) | d=4 (s) |
|---|---|---|---|---|
| Pairs — Combiner | 324 | 415 | 277 | 347 |
| Pairs — In-Mapper | 273 | 295 | 265 | 375 |
| Stripes — Map-based | 79.94 | 109.48 | 125.25 | 223.73 |
| Stripes — In-Mapper | 66.15 | 148.84 | 162.64 | 182.32 |

**Key Takeaway:** Stripes approaches significantly outperform Pairs. In-Mapper combining is fastest at small `d` but introduces memory overhead at larger window sizes.

**Run (Pairs In-Mapper):**
```bash
hadoop jar jars/CoOccurence3.jar <input_path> <output_path> <top50_path> <d>
```

**Run (Stripes In-Mapper):**
```bash
hadoop jar jars/stripe_inmap.jar <input_path> <output_path> <top50_path> <stopwords_path> <d>
```

---

## Problem 2 — Document Frequency & TF-IDF

### Part (a): Document Frequency (DF)

Computes the number of unique documents each term appears in across the full Wikipedia corpus. Applies Porter Stemming and stopword filtering. Outputs a TSV of all terms and their DF values, then extracts the top 100 terms by DF.

**Run:**
```bash
hadoop jar jars/2a.jar DFWordCount \
  /user/<user>/input/Wikipedia-EN-20120601_ARTICLES \
  /user/<user>/output/df_output \
  /user/<user>/stopwords.txt
```

**Extract Top 100:**
```bash
cat df_output_hdfs/part-r-00000 | sort -k2 -nr | head -100 > df_top100_hdfs.tsv
```

**Sample Output:**

| Term | DF |
|---|---|
| s | 9836 |
| apo | 9518 |
| categori | 9381 |
| refer | 9340 |
| quot | 8674 |

**Execution Summary:**
- Total documents processed: 10,000
- Total unique terms: 536,851
- Map output records: 7,157,718

---

### Part (b): TF-IDF Scoring

For each of the top 100 terms (from Part a), computes TF-IDF scores for every document using the formula:

```
SCORE = TF × log(10000 / DF + 1)
```

Uses the **Stripes approach** — one stripe per document bounded to 100 terms — making the shuffle phase very compact (~3.47 MB for 10,000 documents). DF values are distributed via Hadoop's Distributed Cache.

**Run:**
```bash
hadoop jar jars/assign2b.jar TFIDF \
  /user/<user>/input \
  /user/<user>/output \
  /user/<user>/top100_df.tsv
```

**Output Format:**
```
ID        TERM        SCORE
100005    http        11.096731
100005    apo         7.899676
100005    www         4.531053
```

**Execution Summary (Full Dataset):**

| Metric | Value |
|---|---|
| Map input records | 10,000 |
| Reduce output records | 468,506 |
| Output file size | ~9.2 MB |
| Runtime | ~117 minutes (single-node) |

---

## Setup & Prerequisites

- Java 11+
- Apache Hadoop 3.x
- YARN enabled
- `opennlp-tools-1.9.3.jar` (for Porter Stemming, bundled in JARs)

**Start Hadoop:**
```bash
start-dfs.sh
start-yarn.sh
jps   # verify daemons
```

**Upload Data to HDFS:**
```bash
hdfs dfs -mkdir -p /user/<user>/input
hdfs dfs -put Wikipedia-EN-20120601_ARTICLES /user/<user>/input/
hdfs dfs -put stopwords.txt /user/<user>/
hdfs dfs -getmerge /user/$USER/input/Wikipedia-EN-20120601_ARTICLES /tmp/merged_input.txt
hdfs dfs -mkdir -p /user/$USER/input_merged
hdfs dfs -put /tmp/merged_input.txt /user/$USER/input_merged/
```

---

## Conclusion

The Stripes approach consistently outperforms Pairs in both runtime and shuffle efficiency due to reduced intermediate key emissions. TF-IDF scoring via distributed caching and the Stripes algorithm scales well even on a single-node cluster, and is designed to scale linearly across multi-node deployments.
