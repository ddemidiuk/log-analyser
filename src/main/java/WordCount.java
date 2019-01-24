import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * Hadoop WordCount first program
 */
public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "wordcount");
        job.setJarByClass(getClass());

        TextInputFormat.addInputPath(job, new Path("hdfs:///tmp/denis/log/access_log.tsv"));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Reduce.class);

        TextOutputFormat.setOutputPath(job, new Path("hdfs:///tmp/denis/log/result"));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String textLine = value.toString();
            try {
                URL url = new URL(textLine);
                //  while (tokenizer.hasMoreTokens()) {
                HostEnum host = HostEnum.getByHost(url.getHost());
                if (host != null) {
                    List<NameValuePair> params = URLEncodedUtils.parse(url.toURI(), "UTF-8");
                    String query = params.stream()
                            .filter(pair -> pair.getName().equals(host.getQueryParamName()))
                            .findFirst()
                            .get()
                            .getValue();

                    word.set(host.getHost() + "   " + query);
                    context.write(word, one);
                } else {
                    word.set("empty");
                    context.write(word, one);
                }
                //}
            } catch (Exception e) {
                System.err.println("Can't parse line \" " + textLine + "\" to URL");
                word.set("error: " + e.toString());
                context.write(word, one);
            }

        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.getCounter("Reduce", "Words").increment(1);
            context.write(key, new IntWritable(sum));
        }
    }
}