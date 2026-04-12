import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.Text;

public class StripeMapFunction {

    
    public static class MapWritableStripe implements Writable {
        private final Map<String, Integer> counts = new HashMap<>();

        public void increment(String k, int v) {
            counts.merge(k, v, Integer::sum);
        }

        public void merge(MapWritableStripe other) {
            for (Map.Entry<String, Integer> e : other.counts.entrySet()) {
                counts.merge(e.getKey(), e.getValue(), Integer::sum);
            }
        }

        public boolean isEmpty() {
            return counts.isEmpty();
        }

        public Map<String, Integer> getCounts() {
            return counts;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(counts.size());
            for (Map.Entry<String, Integer> e : counts.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeInt(e.getValue());
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            counts.clear();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                counts.put(in.readUTF(), in.readInt());
            }
        }

        public String toString() {
            return counts.toString();
        }
    }

    
    public static class MapperClass
            extends Mapper<Object, Text, Text, MapWritableStripe> {

        private Set<String> stopWords = new HashSet<>();
        private int d;

        @Override
        protected void setup(Context context) throws IOException {

            URI[] files = context.getCacheFiles();

            if (files != null) {
                for (URI uri : files) {

                    File file = new File(new Path(uri.getPath()).getName());

                    BufferedReader br = new BufferedReader(new FileReader(file));
                    String line;

                    while ((line = br.readLine()) != null) {
                        stopWords.add(line.trim().toLowerCase());
                    }

                    br.close();
                }
            }

            d = context.getConfiguration().getInt("window", 1);
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] words = value.toString().toLowerCase().split("\\W+");

            Map<String, MapWritableStripe> local = new HashMap<>();

            for (int i = 0; i < words.length; i++) {
                String w = words[i];
                if (w.isEmpty() || stopWords.contains(w))
                    continue;

                MapWritableStripe stripe = local.getOrDefault(w, new MapWritableStripe());

                for (int j = Math.max(0, i - d); j <= Math.min(words.length - 1, i + d); j++) {
                    if (i == j)
                        continue;
                    String n = words[j];
                    if (n.isEmpty() || stopWords.contains(n))
                        continue;

                    stripe.increment(n, 1);
                }

                local.put(w, stripe);
            }

            for (Map.Entry<String, MapWritableStripe> e : local.entrySet()) {
                context.write(new Text(e.getKey()), e.getValue());
            }
        }
    }

    
    public static class Combiner
            extends Reducer<Text, MapWritableStripe, Text, MapWritableStripe> {

        public void reduce(Text key, Iterable<MapWritableStripe> values, Context context)
                throws IOException, InterruptedException {

            MapWritableStripe result = new MapWritableStripe();
            for (MapWritableStripe v : values)
                result.merge(v);

            context.write(key, result);
        }
    }

    
    public static class ReducerClass
            extends Reducer<Text, MapWritableStripe, Text, Text> {

        public void reduce(Text key, Iterable<MapWritableStripe> values, Context context)
                throws IOException, InterruptedException {

            MapWritableStripe result = new MapWritableStripe();
            for (MapWritableStripe v : values)
                result.merge(v);

            context.write(key, new Text(result.toString()));
        }
    }

    
    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.err.println("Usage: <input> <output> <stopwords> <window>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        
        conf.set("mapreduce.map.memory.mb", "4096");
        conf.set("mapreduce.map.java.opts", "-Xmx3500m");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.reduce.java.opts", "-Xmx3500m");

        
        conf.set("mapreduce.task.io.sort.mb", "256");
        conf.set("mapreduce.map.sort.spill.percent", "0.80");

        
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec",
                "org.apache.hadoop.io.compress.SnappyCodec");

        
        conf.setInt("window", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "Stripe MapFunction d=" + args[3]);

        job.setJarByClass(StripeMapFunction.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritableStripe.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}