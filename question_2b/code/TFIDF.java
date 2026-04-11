import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import opennlp.tools.stemmer.PorterStemmer;

public class TFIDF {

    public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Map<String, Integer> dfMap = new HashMap<>();
        private final PorterStemmer stemmer = new PorterStemmer();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    java.io.File f = new java.io.File(new Path(uri.getPath()).getName());
                    if (!f.exists()) f = new java.io.File(uri.getPath());
                    loadDFFile(f.getAbsolutePath());
                }
            }
            System.out.println("DEBUG mapper setup: dfMap size = " + dfMap.size());
        }

        private void loadDFFile(String fileName) {
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        try {
                            dfMap.put(parts[0].trim(), Integer.parseInt(parts[1].trim()));
                        } catch (NumberFormatException e) { }
                    }
                }
            } catch (IOException ioe) {
                System.err.println("Error loading DF: " + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Use filename as docID, entire line as text
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String docID = fileName.replace(".txt", "");

            Map<String, Integer> stripe = new HashMap<>();
            String[] tokens = line.toLowerCase().split("[^a-zA-Z]+");

            for (String token : tokens) {
                if (token.isEmpty()) continue;
                String stemmed = stemmer.stem(token);
                if (dfMap.containsKey(stemmed)) {
                    stripe.merge(stemmed, 1, Integer::sum);
                }
            }

            if (stripe.isEmpty()) return;

            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> e : stripe.entrySet()) {
                if (sb.length() > 0) sb.append(",");
                sb.append(e.getKey()).append(":").append(e.getValue());
            }

            context.write(new Text(docID), new Text(sb.toString()));
        }
    }

    public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

        private final Map<String, Integer> dfMap = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    java.io.File f = new java.io.File(new Path(uri.getPath()).getName());
                    if (!f.exists()) f = new java.io.File(uri.getPath());
                    loadDFFile(f.getAbsolutePath());
                }
            }
        }

        private void loadDFFile(String fileName) {
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        try {
                            dfMap.put(parts[0].trim(), Integer.parseInt(parts[1].trim()));
                        } catch (NumberFormatException e) { }
                    }
                }
            } catch (IOException ioe) {
                System.err.println("Error loading DF: " + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void reduce(Text docID, Iterable<Text> stripes, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> merged = new HashMap<>();
            for (Text stripe : stripes) {
                for (String entry : stripe.toString().split(",")) {
                    String[] kv = entry.split(":");
                    if (kv.length == 2) {
                        try {
                            merged.merge(kv[0], Integer.parseInt(kv[1]), Integer::sum);
                        } catch (NumberFormatException e) { }
                    }
                }
            }

            for (Map.Entry<String, Integer> e : merged.entrySet()) {
                String term = e.getKey();
                int tf = e.getValue();
                int df = dfMap.getOrDefault(term, 1);
                double score = tf * Math.log((10000.0 / df) + 1.0);
                context.write(docID, new Text(term + "\t" + String.format("%.6f", score)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Number of args: " + args.length);
        for (int i = 0; i < args.length; i++) {
            System.out.println("args[" + i + "] = " + args[i]);
        }

        if (args.length < 4) {
            System.err.println("Usage: TFIDF <input> <output> <top100-df-tsv>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "tfidf-stripes");

        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[3]).toUri());
        job.setInputFormatClass(CustomFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}