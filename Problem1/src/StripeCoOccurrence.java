import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripeCoOccurrence {

    // ---------- CUSTOM WRITABLE ----------
    public static class MapWritableStripe extends MapWritable {
        public void increment(Text key, int count) {
            IntWritable val = (IntWritable) this.get(key);
            if (val == null) {
                this.put(key, new IntWritable(count));
            } else {
                val.set(val.get() + count);
            }
        }
    }

    // ---------- MAPPER ----------
    public static class StripeMapper extends Mapper<Object, Text, Text, MapWritableStripe> {

        private Set<String> stopWords = new HashSet<>();
        private int d;

        @Override
        protected void setup(Context context) throws IOException {

            // Load stopwords (from distributed cache)
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    File file = new File(new Path(uri.getPath()).getName());
                    BufferedReader reader = new BufferedReader(new FileReader(file));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stopWords.add(line.trim().toLowerCase());
                    }
                    reader.close();
                }
            }

            d = context.getConfiguration().getInt("window", 1);
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().toLowerCase().split("[^\\w']+");

            for (int i = 0; i < tokens.length; i++) {

                String w = tokens[i];
                if (w.length() == 0 || stopWords.contains(w)) continue;

                MapWritableStripe stripe = new MapWritableStripe();

                for (int j = Math.max(0, i - d); j <= Math.min(tokens.length - 1, i + d); j++) {
                    if (j == i) continue;

                    String neighbor = tokens[j];
                    if (neighbor.length() == 0 || stopWords.contains(neighbor)) continue;

                    stripe.increment(new Text(neighbor), 1);
                }

                if (!stripe.isEmpty()) {
                    context.write(new Text(w), stripe);
                }
            }
        }
    }

    // ---------- REDUCER ----------
    public static class StripeReducer extends Reducer<Text, MapWritableStripe, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<MapWritableStripe> values, Context context)
                throws IOException, InterruptedException {

            MapWritableStripe result = new MapWritableStripe();

            for (MapWritableStripe stripe : values) {
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                    Text k = (Text) entry.getKey();
                    IntWritable v = (IntWritable) entry.getValue();

                    result.increment(k, v.get());
                }
            }

            context.write(key, new Text(result.toString()));
        }
    }

    // ---------- DRIVER ----------
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInt("window", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "Stripe CoOccurrence");

        job.setJarByClass(StripeCoOccurrence.class);

        job.setMapperClass(StripeMapper.class);
        job.setReducerClass(StripeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritableStripe.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Distributed cache
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}