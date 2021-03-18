import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

public class YouTubeAnalysis {

    //mapper class
    public static class YTMapper
            extends Mapper<LongWritable, Text, Text, LongWritable>
    {

        private Text videoid = new Text();
        private LongWritable views = new LongWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String[] arr = value.toString().split("\\s");

            if ((arr.length >= 10) && arr[5].matches("[0-9]+")){
                videoid.set(arr[0]);
                views.set(Long.parseLong(arr[5]));
            }
            else if (arr.length == 1){
                videoid.set(arr[0]);
                views.set(0);
            }
            context.write(videoid,views);
        }
    }

    //reducer class
    public static class YTReducer
            extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException
        {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    //driver class
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "YouTube Analysis");
        job.setJarByClass(YouTubeAnalysis.class);
        job.setMapperClass(YTMapper.class);
        job.setCombinerClass(YTReducer.class);
        job.setReducerClass(YTReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
