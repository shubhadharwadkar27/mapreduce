import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class partition1 {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text> {
		Text data = new Text();
		public void map (LongWritable key ,Text value,Context context) throws IOException, InterruptedException{
			String part[] = value.toString().split(",");
			String gender = part[3];
			context.write(new Text(gender),new Text(value));
		} 
	}
	public static class PartPartitioner extends Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str = value.toString().split(",");
	         int age = Integer.parseInt(str[2]);
	         
	         if(age<=20) return 0;
	         else if(age>20 && age<=30) return 1;
	         else return 2;
	      }
	   }
	public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>{
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			int maxSalary = 0;
			for (Text val : value){
				String part[] = val.toString().split(",");
				int salary = Integer.parseInt(part[4]);
				if(salary>maxSalary)maxSalary=salary;
			}
			context.write(new Text(key),new IntWritable(maxSalary) );
		}
	}
	public static void main(String args[]) throws Exception {
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf, "PartitionText");
		  job.setJarByClass(partition1.class);
		  job.setMapperClass(MapClass.class);
		  job.setReducerClass(ReduceClass.class);
		  job.setPartitionerClass(PartPartitioner.class);
		  job.setNumReduceTasks(3);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(Text.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class);
		  //job.setOutputValueClass(NullWritable.class);
		  //job.setInputFormatClass(TextInputFormat.class);
		  //job.setOutputFormatClass(TextOutputFormat.class);
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);

		 }

}
