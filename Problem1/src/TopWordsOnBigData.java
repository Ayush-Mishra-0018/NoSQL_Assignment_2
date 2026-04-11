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

public class TopWordsOnBigData {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopwords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI uri : cacheFiles) {
                    // Correctly access the cached file by its name in the local work directory
                    try (BufferedReader reader = new BufferedReader(new FileReader(new Path(uri.getPath()).getName()))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            stopwords.add(line.trim().toLowerCase());
                        }
                    }
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Only convert to lowercase; do not strip numbers or special URL characters unless they are delimiters
            String line = value.toString().toLowerCase();
            // Split by whitespace to preserve URLs and internal alphanumeric structures
            String[] tokens = line.split("\\s+");

            for (String token : tokens) {
                // Remove trailing/leading punctuation but keep the core token (handles "word," or "(word)")
                token = token.replaceAll("^[^a-z0-9]+|[^a-z0-9]+$", "");
                
                if (!token.isEmpty() && !stopwords.contains(token)) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    public static class Top50Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Use a TreeMap to sort by frequency (descending)
        private TreeMap<Integer, List<String>> topMap = new TreeMap<>(Collections.reverseOrder());

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // Temporarily store in memory to find the global top 50
            topMap.computeIfAbsent(sum, k -> new ArrayList<>()).add(key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Map.Entry<Integer, List<String>> entry : topMap.entrySet()) {
                for (String word : entry.getValue()) {
                    if (count >= 50) break;
                    context.write(new Text(word), new IntWritable(entry.getKey()));
                    count++;
                }
                if (count >= 50) break;
            }
        }
    }

    // Standard Reducer for the Combiner phase to optimize "Big Data" processing
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TopWordsOnBigData <input> <output> <stopwords>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Words Big Data");
        job.setJarByClass(TopWordsOnBigData.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class); // CRITICAL: Local aggregation for performance
        job.setReducerClass(Top50Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Distributed cache for stopwords
        job.addCacheFile(new Path(args[2]).toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}