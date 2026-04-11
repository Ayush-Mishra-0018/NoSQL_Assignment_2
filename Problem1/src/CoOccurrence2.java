import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CoOccurrence2 {

    public static class PairsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Set<String> top50Words = new HashSet<>();
        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();
        private int neighborDistance;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            // Get distance d from configuration
            neighborDistance = conf.getInt("neighbor.distance", 2);

            // Load top 50 words from Distributed Cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheUri : cacheFiles) {
                    Path path = new Path(cacheUri.getPath());
                    parseTopWords(path.getName());
                }
            }
        }

        private void parseTopWords(String fileName) {
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Splits based on the tab in your a.txt 
                    String[] parts = line.split("\\s+");
                    if (parts.length > 0) {
                        top50Words.add(parts[0].trim().toLowerCase());
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading top 50 words: " + e.getMessage());
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Standardizing text processing as per Part A
            String line = value.toString().toLowerCase().replaceAll("[^a-z0-9\\s]", " ");
            String[] tokens = line.split("\\s+");

            for (int i = 0; i < tokens.length; i++) {
                String word = tokens[i];

                // Check if current word is one of the frequent words 
                if (top50Words.contains(word)) {
                    int start = Math.max(0, i - neighborDistance);
                    int end = Math.min(tokens.length - 1, i + neighborDistance);

                    for (int j = start; j <= end; j++) {
                        if (i == j) continue; // Skip self

                        String neighbor = tokens[j];
                        // Filter for co-occurrence only with other top 50 words 
                        if (top50Words.contains(neighbor)) {
                            // Pairs approach: Emit (word, neighbor) 
                            pair.set("(" + word + "," + neighbor + ")");
                            context.write(pair, one);
                        }
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: CoOccurrence2 <input> <output> <top50_path> <distance_d>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("neighbor.distance", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "Co-Occurrence Matrix Pairs d=" + args[3]);
        job.setJarByClass(CoOccurrence2.class);

        job.setMapperClass(PairsMapper.class);
        // Requirement: Local aggregation at Map-function level (Combiner) [cite: 19]
        job.setCombinerClass(IntSumReducer.class); 
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Add the top 50 words file to distributed cache [cite: 15]
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}