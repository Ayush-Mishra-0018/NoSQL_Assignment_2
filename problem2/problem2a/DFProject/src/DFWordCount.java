import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import opennlp.tools.stemmer.PorterStemmer;

public class DFWordCount {

    // ===================== MAPPER =====================
    public static class DFMapper extends Mapper<Object, Text, Text, Text> {

        private final Set<String> stopwords = new HashSet<>();
        private final Set<String> uniqueWords = new HashSet<>();
        private final PorterStemmer stemmer = new PorterStemmer();

        private final Text wordText = new Text();
        private final Text docIdText = new Text();

        private String docId;

        @Override
        protected void setup(Context context) throws IOException {

            uniqueWords.clear();

            // Load stopwords using Distributed Cache
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    Path path = new Path(uri.getPath());
                    String fileName = path.getName();

                    if (fileName.equals("stopwords.txt")) {
                        BufferedReader reader = new BufferedReader(new FileReader(fileName));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            stopwords.add(line.trim().toLowerCase());
                        }
                        reader.close();
                    }
                }
            }

            // Extract document ID (filename)
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            docId = fileSplit.getPath().getName();
        }

        @Override
        public void map(Object key, Text value, Context context) {

            String line = value.toString().toLowerCase();

            // Split tokens
            String[] tokens = line.split("[^\\w']+");

            for (String token : tokens) {

                if (token.isEmpty()) continue;

                // Only stopword filtering (as per requirement)
                if (stopwords.contains(token)) continue;

                // Stemming
                token = stemmer.stem(token).toString();

                uniqueWords.add(token);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            // Emit each word once per document
            for (String word : uniqueWords) {
                wordText.set(word);
                docIdText.set(docId);
                context.write(wordText, docIdText);
            }
        }
    }

    // ===================== REDUCER =====================
    public static class DFReducer extends Reducer<Text, Text, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Set<String> uniqueDocs = new HashSet<>();

            for (Text val : values) {
                uniqueDocs.add(val.toString());
            }

            result.set(uniqueDocs.size());
            context.write(key, result);
        }
    }

    // ===================== DRIVER =====================
    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: DFWordCount <input> <output> <stopwords>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Document Frequency");

        job.setJarByClass(DFWordCount.class);

        job.setMapperClass(DFMapper.class);
        job.setReducerClass(DFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(CustomFileInputFormat.class);

        // Add stopwords file to distributed cache
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}