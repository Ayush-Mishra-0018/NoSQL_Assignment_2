import java.io.*;
import java.net.URI;
import java.util.*;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Text;

public class StripeCoOccurrence {

    // ---------- CUSTOM WRITABLE ----------
    /**
     * A stripe is a map from neighbor-word → co-occurrence count.
     *
     * BUG FIX: MapWritable uses object-reference equality for lookups,
     * so we MUST reuse a canonical Text key (or use a plain HashMap
     * internally and serialize manually).  We use a HashMap<String,int>
     * internally and only convert to/from Writable for serialization.
     */
    public static class MapWritableStripe implements Writable {

        // Use plain HashMap so key lookup works correctly (String.equals)
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

        // --- Writable serialization ---
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
            // Produce a readable "{word=count, word=count, ...}" string
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            // Sort for deterministic output
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
        private int d;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load stop-words from distributed cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    File file = new File(new Path(uri.getPath()).getName());
                    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String w = line.trim().toLowerCase();
                            if (!w.isEmpty()) stopWords.add(w);
                        }
                    }
                }
            }
            d = context.getConfiguration().getInt("window", 1);
        }

        private final Text outKey = new Text();           // reuse objects
        private final MapWritableStripe outVal = new MapWritableStripe();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Tokenise: keep apostrophes inside words, drop everything else
            String[] tokens = value.toString().toLowerCase().split("[^\\w']+");

            // Pre-filter tokens once to avoid repeated checks inside the inner loop
            String[] clean = new String[tokens.length];
            for (int i = 0; i < tokens.length; i++) {
                String t = tokens[i];
                clean[i] = (t.isEmpty() || stopWords.contains(t)) ? null : t;
            }

            for (int i = 0; i < clean.length; i++) {
                if (clean[i] == null) continue;

                // BUG FIX: clear and reuse the stripe object instead of
                //          allocating a new MapWritableStripe per word.
                outVal.getCounts().clear();

                int start = Math.max(0, i - d);
                int end   = Math.min(clean.length - 1, i + d);

                for (int j = start; j <= end; j++) {
                    if (j == i || clean[j] == null) continue;
                    // BUG FIX: String key → HashMap.get() works correctly
                    outVal.increment(clean[j], 1);
                }

                if (!outVal.isEmpty()) {
                    outKey.set(clean[i]);
                    context.write(outKey, outVal);
                }
            }
        }
    }

    // ---------- COMBINER ----------
    /**
     * BUG FIX: The original code used StripeReducer as Combiner.
     * A Combiner MUST have the same output types as the Mapper
     * (<Text, MapWritableStripe>), NOT <Text, Text>.
     * Using the wrong type caused a serialization crash → no output.
     */
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

        if (args.length < 4) {
            System.err.println(
                "Usage: StripeCoOccurrence <input> <output> <stopwords> <window>");
            System.exit(2);
        }

        // BUG FIX: Build Configuration BEFORE Job.getInstance() so that
        //          all task-JVM settings are actually picked up.
        Configuration conf = new Configuration();

        // Memory for map / reduce task containers
        conf.set("mapreduce.map.memory.mb",    "2048");
        conf.set("mapreduce.map.java.opts",    "-Xmx1800m -XX:+UseG1GC");
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.set("mapreduce.reduce.java.opts", "-Xmx1800m -XX:+UseG1GC");

        // Sort buffer — keep modest so spills happen before OOM
        conf.set("mapreduce.task.io.sort.mb",          "256");
        conf.set("mapreduce.map.sort.spill.percent",   "0.80");

        // Intermediate compression (reduces shuffle traffic significantly)
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec",
                 "org.apache.hadoop.io.compress.SnappyCodec");

        // Window size (d)
        conf.setInt("window", Integer.parseInt(args[3]));

 

        Job job = Job.getInstance(conf, "Stripe CoOccurrence d=" + args[3]);

        job.setJarByClass(StripeCoOccurrence.class);

        job.setMapperClass(StripeMapper.class);
        job.setCombinerClass(StripeCombiner.class);   
        job.setReducerClass(StripeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritableStripe.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Distributed cache for stop-words
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job,  new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}