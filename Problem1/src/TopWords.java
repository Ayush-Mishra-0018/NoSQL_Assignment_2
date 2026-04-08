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

public class TopWords {

    public static class MapperClass extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"));
            String line;
            while ((line = br.readLine()) != null) {
                stopWords.add(line.trim().toLowerCase());
            }
            br.close();
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            // 🔥 CLEANING (MOST IMPORTANT)
            line = line.replaceAll("http\\S+", " ");
            line = line.replaceAll("&\\w+;", " ");
            line = line.replaceAll("[^a-z ]", " ");

            String[] tokens = line.split("\\s+");

            for (String token : tokens) {
                if (token.length() < 3) continue;
                if (stopWords.contains(token)) continue;

                word.set(token);
                context.write(word, one);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {

        private PriorityQueue<WordFreq> pq = new PriorityQueue<>();

        static class WordFreq implements Comparable<WordFreq> {
            String word;
            int freq;

            WordFreq(String w, int f) {
                word = w;
                freq = f;
            }

            public int compareTo(WordFreq o) {
                return Integer.compare(this.freq, o.freq); // min-heap
            }
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {

            int sum = 0;
            for (IntWritable val : values) sum += val.get();

            pq.add(new WordFreq(key.toString(), sum));
            if (pq.size() > 50) pq.poll(); // keep only top 50
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            List<WordFreq> list = new ArrayList<>(pq);
            list.sort((a, b) -> b.freq - a.freq); // descending

            for (WordFreq wf : list) {
                context.write(new Text(wf.word), new IntWritable(wf.freq));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 50 Words");

        job.setJarByClass(TopWords.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setNumReduceTasks(1); // 🔥 IMPORTANT

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new URI(args[2]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}