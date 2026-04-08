import java.io.*;
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

public class CoOccurrence {

    public static class MapperClass extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();

        private Set<String> topWords = new HashSet<>();
        private int distance;

        @Override
        protected void setup(Context context) throws IOException {

            Configuration conf = context.getConfiguration();
            distance = conf.getInt("co.distance", 1);

            BufferedReader br = new BufferedReader(new FileReader("top50.txt"));
            String line;

            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+");
                topWords.add(parts[0]); // word
            }

            br.close();
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            line = line.replaceAll("http\\S+", " ");
            line = line.replaceAll("&\\w+;", " ");
            line = line.replaceAll("[^a-z ]", " ");

            String[] tokens = line.split("\\s+");

            List<String> words = new ArrayList<>();

            for (String token : tokens) {
                if (topWords.contains(token)) {
                    words.add(token);
                }
            }

            for (int i = 0; i < words.size(); i++) {
                for (int j = i + 1; j <= i + distance && j < words.size(); j++) {

                    String w1 = words.get(i);
                    String w2 = words.get(j);

                    pair.set(w1 + "," + w2);
                    context.write(pair, one);

                    pair.set(w2 + "," + w1);
                    context.write(pair, one);
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) sum += val.get();

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.setInt("co.distance", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "CoOccurrence");

        job.setJarByClass(CoOccurrence.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new URI(args[2])); // top50.txt

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}