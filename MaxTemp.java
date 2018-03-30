/*
 *  Finds the mean maximum temperature for everyday of the year for each weather station
 *
 *  To compile:
 *  hadoop com.sun.tools.javac.Main MaxTemp.java
 *  jar cf mt.jar MaxTemp*.class
 *
 *  To run:
 *  hadoop jar mt.jar MaxTemp <input path> <output path for first job> <final output path>
 *  - this program runs 2 MapReduce jobs to get the final output, so you will need two output paths
 *
 *  My implementation was built off the example code provided at /user/chatree/CS1699/MaxTemp
 *
 *
 *  James Huang
 *
 */
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemp {

  public static class MaxTempMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String line = value.toString();
      String date = line.substring(15, 23);
      String station = line.substring(4, 15);
      int airTemperature;
      if (line.charAt(87) == '+') {
        airTemperature = Integer.parseInt(line.substring(88, 92));
      } else {
        airTemperature = Integer.parseInt(line.substring(87, 92));
      }
      String quality = line.substring(92, 93);
      if (airTemperature != MISSING && quality.matches("[01459]")) {
        context.write(new Text(station+ " " + date), new IntWritable(airTemperature));
      }
    }
  }

  public static class MaxTempReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context)
        throws IOException, InterruptedException {

      int maxValue = Integer.MIN_VALUE;
      int count = 0;
      int total = 0;

      for (IntWritable value : values) {
        maxValue = Math.max(maxValue, value.get());
      }

      context.write(key, new IntWritable(maxValue));
    }
  }

  public static class MaxMeanMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String line = value.toString();
      String date = line.substring(16, 20);
      String station = line.substring(0, 11);
      int airTemperature;
      if (line.charAt(21) == '+') { // parseInt doesn't like leading plus signs
        airTemperature = Integer.parseInt(line.substring(22, line.length()));
      } else {
        airTemperature = Integer.parseInt(line.substring(21, line.length()));
      }

      context.write(new Text(station+ " " + date), new IntWritable(airTemperature));

    }
  }

  public static class MaxMeanReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context)
        throws IOException, InterruptedException {

      int maxValue = Integer.MIN_VALUE;
      int count = 0;
      int total = 0;

      for (IntWritable value : values) {
        total = total + value.get();
        count++;
      }

      context.write(key, new IntWritable(total/count));
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: MaxTemp <input path> <output path for first job> <final output path>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(MaxTemp.class);
    job.setJobName("Max temperature");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(MaxTempMapper.class);
    job.setReducerClass(MaxTempReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    Job job2 = null;
    //wait for first job to be completed before starting second job
    if (job.waitForCompletion(true)) {
      job2 = new Job();
      job2.setJarByClass(MaxTemp.class);
      job2.setJobName("Max temperature");
      FileInputFormat.addInputPath(job2, new Path(args[1]));
      FileOutputFormat.setOutputPath(job2, new Path(args[2]));
      job2.setMapperClass(MaxMeanMapper.class);
      job2.setReducerClass(MaxMeanReducer.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(IntWritable.class);
    }

    System.exit((job2.waitForCompletion(true) ? 0 : 1));
  }
}
