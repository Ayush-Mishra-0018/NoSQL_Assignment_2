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

public class TopWords2 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Requirement: Use Distributed Cache to eliminate stop-words
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheUri : cacheFiles) {
                    Path path = new Path(cacheUri.getPath());
                    parseStopWords(path.getName());
                }
            }
        }

        private void parseStopWords(String fileName) {
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
            } catch (IOException e) {
                System.err.println("Error reading stop-words: " + e.getMessage());
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Normalize text: lowercase and remove non-alphanumeric
            String line = value.toString().toLowerCase().replaceAll("[^a-z0-9\\s]", " ");
            StringTokenizer itr = new StringTokenizer(line);
            
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                // Exclude stop-words
                if (!stopWords.contains(token) && token.length() > 1) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    // Combiner for local aggregation
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class Top50Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // TreeMap to identify the top 50 most frequent words
        private TreeMap<Integer, List<String>> countMap = new TreeMap<>(Collections.reverseOrder());

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            countMap.computeIfAbsent(sum, k -> new ArrayList<>()).add(key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Map.Entry<Integer, List<String>> entry : countMap.entrySet()) {
                for (String word : entry.getValue()) {
                    if (count >= 50) break; //
                    context.write(new Text(word), new IntWritable(entry.getKey()));
                    count++;
                }
                if (count >= 50) break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TopWords2 <input> <output> <stopwords_path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 50 Frequent Words");
        job.setJarByClass(TopWords2.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class); 
        job.setReducerClass(Top50Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Add stopwords to Distributed Cache
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}