import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Qu9 {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value ,Context context) throws IOException, InterruptedException{
			String parts[] = value.toString().split(";");
			//float unit = Float.parseFloat(parts[6]);
			//float cost = Float.parseFloat(parts[7]);
			//double avg = cost /unit;
			String output = parts[5]+","+parts[7]+","+parts[6];
			context.write(new Text(parts[5]), new Text(output));
		}
	}
	public static class ReduceClass extends Reducer<Text,Text,Text,DoubleWritable>{
		public void reduce(Text key,Iterable<Text>value,Context context) throws IOException, InterruptedException{
			double sum = 0;
			double totalavg = 0;
			for (Text val : value){
				String parts[] = val.toString().split(",");
				
				totalavg += Float.parseFloat(parts[1]);
				sum += Float.parseFloat(parts[2]);
			}
			double avg = totalavg/sum;
			context.write(key, new DoubleWritable(avg));
		}
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top Record for largest amount spent");
	    job.setJarByClass(Qu9.class);
	    job.setMapperClass(MapClass.class);
	    job.setReducerClass(ReduceClass.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
