
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlightAnalysis3 {

    public static class flightMapper extends Mapper<Object,Text,Text,Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String row = value.toString();
            String [] arr = row.split(",");
            Text airline_code = new Text(arr[2]);

            if(!arr[8].equals("")){
                context.write(airline_code, new Text("Delay="+arr[8]));
            }
        }

    }

    public static class airlineMapper extends Mapper<Object,Text,Text,Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String row = value.toString();
            String [] arr = row.split(",");
            Text airline_code = new Text(arr[0]);
            Text airline = new Text(arr[1]);
            System.out.println("AirlineMapper"+airline_code+" "+airline);
            context.write(airline_code,airline);

        }
    }

    public static class flightReducer extends Reducer<Text, Text, Text, IntWritable>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int delayed = 0;
            String name = null;
            for(Text t : values){

                String input;
                input = t.toString();

                if (input.contains("Delay")){

                    String[] arr = input.split("=");
                    if(Integer.parseInt(arr[1])>0)
                    {
                        delayed++;
                    }
                }
                else{
                    name = input;
                }
            }
            Text airline = new Text(name);
            IntWritable NoOfDelays = new IntWritable(delayed);

            context.write(airline, NoOfDelays);
        }
    }

    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Flight Analysis Job 2");
        job.setJarByClass(FlightAnalysis3.class);
        job.setReducerClass(flightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, flightMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, airlineMapper.class);
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0  : 1);
    }
}
