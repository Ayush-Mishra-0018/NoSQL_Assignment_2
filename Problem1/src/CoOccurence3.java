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

public class CoOccurence3 {

    public static class PairsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Set<String> top50Words = new HashSet<>();
        private Map<String, Integer> inMapperBuffer = new HashMap<>(); // For Map-class aggregation
        private boolean useInMapper;
        private int neighborDistance;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            neighborDistance = conf.getInt("neighbor.distance", 2);
            // Decide if we are doing Map-class level aggregation
            useInMapper = conf.getBoolean("aggregation.mapclass", false);

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
                    String[] parts = line.split("\\s+");
                    if (parts.length > 0) {
                        top50Words.add(parts[0].trim().toLowerCase());
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading top words: " + e.getMessage());
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase().replaceAll("[^a-z0-9\\s]", " ");
            String[] tokens = line.split("\\s+");

            for (int i = 0; i < tokens.length; i++) {
                String word = tokens[i];
                if (top50Words.contains(word)) {
                    int start = Math.max(0, i - neighborDistance);
                    int end = Math.min(tokens.length - 1, i + neighborDistance);

                    for (int j = start; j <= end; j++) {
                        if (i == j) continue;
                        String neighbor = tokens[j];
                        if (top50Words.contains(neighbor)) {
                            String pairStr = "(" + word + "," + neighbor + ")";
                            
                            if (useInMapper) {
                                // Map-class level aggregation: store in local buffer
                                inMapperBuffer.put(pairStr, inMapperBuffer.getOrDefault(pairStr, 0) + 1);
                            } else {
                                // Standard emission (used for No-Aggregation or Combiner modes)
                                context.write(new Text(pairStr), new IntWritable(1));
                            }
                        }
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (useInMapper) {
                // Emit aggregated counts at the end of the Map task
                for (Map.Entry<String, Integer> entry : inMapperBuffer.entrySet()) {
                    context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
                }
            }
        }
    }

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
        if (args.length < 5) {
            System.err.println("Usage: CoOccurence3 <input> <output> <top50> <distance> <mode>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("neighbor.distance", Integer.parseInt(args[3]));
        
        String mode = args[4].toLowerCase(); // "none", "combiner", or "mapclass"
        conf.setBoolean("aggregation.mapclass", mode.equals("mapclass"));

        Job job = Job.getInstance(conf, "Co-Occurrence Comparison: " + mode + " d=" + args[3]);
        job.setJarByClass(CoOccurence3.class);
        job.setMapperClass(PairsMapper.class);
        
        // Map-function level aggregation requirement
        if (mode.equals("combiner")) {
            job.setCombinerClass(IntSumReducer.class);
        }
        
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}