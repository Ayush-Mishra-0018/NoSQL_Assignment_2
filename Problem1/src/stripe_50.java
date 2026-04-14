import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class stripe_50 {

    public static class MapWritableStripe implements Writable {

        private final Map<String, Integer> counts = new HashMap<>();

        public void increment(String neighbor, int delta) {
            counts.merge(neighbor, delta, Integer::sum);
        }

        public Map<String, Integer> getCounts() {
            return counts;
        }

        public void merge(MapWritableStripe other) {
            for (Map.Entry<String, Integer> e : other.counts.entrySet()) {
                counts.merge(e.getKey(), e.getValue(), Integer::sum);
            }
        }

        public boolean isEmpty() {
            return counts.isEmpty();
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
                String k = in.readUTF();
                int v = in.readInt();
                counts.put(k, v);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            List<String> keys = new ArrayList<>(counts.keySet());
            Collections.sort(keys);
            for (String k : keys) {
                if (!first) sb.append(", ");
                sb.append(k).append("=").append(counts.get(k));
                first = false;
            }
            sb.append("}");
            return sb.toString();
        }
    }

    // ---------- MAPPER ----------
    public static class StripeMapper
            extends Mapper<Object, Text, Text, MapWritableStripe> {

        private final Set<String> stopWords = new HashSet<>();
        private final Set<String> top50Words = new HashSet<>();
        private int d;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles == null || cacheFiles.length < 2) {
                throw new IOException(
                    "Expected 2 cache files (stopwords, words) but got: " +
                    (cacheFiles == null ? 0 : cacheFiles.length));
            }

            for (URI uri : cacheFiles) {
                // Path(uri.getPath()).getName() gives just the filename
                String fileName = new Path(uri.getPath()).getName();
                File file = new File(fileName);

                if (!file.exists()) {
                    throw new IOException("Cache file not found locally: " + fileName);
                }

                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    if (fileName.contains("stopwords")) {
                        while ((line = reader.readLine()) != null) {
                            String w = line.trim().toLowerCase();
                            if (!w.isEmpty()) stopWords.add(w);
                        }
                    } else {
                        // words.txt — first token on each line is the word
                        while ((line = reader.readLine()) != null) {
                            String[] parts = line.split("\\s+");
                            if (parts.length > 0) {
                                String w = parts[0].trim().toLowerCase();
                                if (!w.isEmpty()) top50Words.add(w);
                            }
                        }
                    }
                }
            }

            if (top50Words.isEmpty()) {
                throw new IOException(
                    "top50Words is empty after loading cache files. " +
                    "Check that words.txt filename does NOT contain 'stopwords'.");
            }

            d = context.getConfiguration().getInt("window", 1);
        }

        private final Text outKey = new Text();
        private final MapWritableStripe outVal = new MapWritableStripe();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase().replaceAll("[^a-z0-9\\s]", " ");
            String[] tokens = line.split("\\s+");

            for (int i = 0; i < tokens.length; i++) {

                String word = tokens[i];

                if (!top50Words.contains(word) || stopWords.contains(word))
                    continue;

                outVal.getCounts().clear();

                int start = Math.max(0, i - d);
                int end   = Math.min(tokens.length - 1, i + d);

                for (int j = start; j <= end; j++) {
                    if (i == j) continue;

                    String neighbor = tokens[j];

                    if (top50Words.contains(neighbor) && !stopWords.contains(neighbor)) {
                        outVal.increment(neighbor, 1);
                    }
                }

                if (!outVal.isEmpty()) {
                    outKey.set(word);
                    context.write(outKey, outVal);
                }
            }
        }
    }

    // ---------- COMBINER ----------
    public static class StripeCombiner
            extends Reducer<Text, MapWritableStripe, Text, MapWritableStripe> {

        private final MapWritableStripe merged = new MapWritableStripe();

        @Override
        public void reduce(Text key, Iterable<MapWritableStripe> values, Context context)
                throws IOException, InterruptedException {

            merged.getCounts().clear();
            for (MapWritableStripe stripe : values) {
                merged.merge(stripe);
            }
            context.write(key, merged);
        }
    }

    // ---------- REDUCER ----------
    public static class StripeReducer
            extends Reducer<Text, MapWritableStripe, Text, Text> {

        private final MapWritableStripe result = new MapWritableStripe();
        private final Text outVal = new Text();

        @Override
        public void reduce(Text key, Iterable<MapWritableStripe> values, Context context)
                throws IOException, InterruptedException {

            result.getCounts().clear();
            for (MapWritableStripe stripe : values) {
                result.merge(stripe);
            }
            outVal.set(result.toString());
            context.write(key, outVal);
        }
    }

    // ---------- DRIVER ----------
    public static void main(String[] args) throws Exception {

        // Expected: <input> <output> <stopwords> <window> <words.txt>
        if (args.length < 5) {
            System.err.println(
                "Usage: stripe_50 <input> <output> <stopwords> <window> <words>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        conf.set("mapreduce.map.memory.mb",    "2048");
        conf.set("mapreduce.map.java.opts",    "-Xmx1800m -XX:+UseG1GC");
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.set("mapreduce.reduce.java.opts", "-Xmx1800m -XX:+UseG1GC");

        conf.set("mapreduce.task.io.sort.mb",          "256");
        conf.set("mapreduce.map.sort.spill.percent",   "0.80");

        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec",
                 "org.apache.hadoop.io.compress.SnappyCodec");

        // args[3] = window (d), args[4] = words.txt
        int window = Integer.parseInt(args[3]);
        conf.setInt("window", window);

        Job job = Job.getInstance(conf, "Stripe CoOccurrence d=" + window);

        job.setJarByClass(stripe_50.class);

        job.setMapperClass(StripeMapper.class);
        job.setCombinerClass(StripeCombiner.class);
        job.setReducerClass(StripeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritableStripe.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // args[2] = stopwords, args[4] = words.txt
        job.addCacheFile(new Path(args[2]).toUri());
        job.addCacheFile(new Path(args[4]).toUri());

        FileInputFormat.addInputPath(job,  new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}