import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightAnalysis {

    public static class flightMapper extends Mapper<Object, Text, Text, IntWritable> {

        private String month;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String [] data_points = data.split(",");
            String year = data_points[0];
            int month_no = Integer.parseInt(data_points[1]);
            switch (month_no){
                case 1:
                    month = "January";
                    break;
                case 2:
                    month = "February";
                    break;
                case 3:
                    month = "March";
                    break;
                case 4:
                    month = "April";
                    break;
                case 5:
                    month = "May";
                    break;
                case 6:
                    month = "June";
                    break;
                case 7:
                    month = "July";
                    break;
                case 8:
                    month = "August";
                    break;
                case 9:
                    month = "September";
                    break;
                case 10:
                    month = "October";
                    break;
                case 11:
                    month = "November";
                    break;
                case 12:
                    month = "December";
                    break;
            }
            Text YearNMonth = new Text();
            YearNMonth.set(String.format("%s %s", month, year));
            IntWritable cancelled = new IntWritable();
            cancelled.set(Integer.parseInt(data_points[10]));
            context.write(YearNMonth, cancelled);
        }
    }

    public static class flightReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private final IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Flight Analysis");
        job.setJarByClass(FlightAnalysis.class);
        job.setMapperClass(flightMapper.class);
        job.setCombinerClass(flightReducer.class);
        job.setReducerClass(flightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
