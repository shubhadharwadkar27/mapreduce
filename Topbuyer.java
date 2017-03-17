import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Topbuyer{
public static class TopbuyerMap extends Mapper<LongWritable,Text,Text,LongWritable>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(";");
			String custid = parts[1];
			long amount = Long.parseLong(parts[8]);
			
			context.write(new Text(custid),new LongWritable(amount));
		}		
	}
public static class TopbuyerReducer extends Reducer<Text,LongWritable,NullWritable,Text>{
	private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();
	public void reduce (Text key,Iterable<LongWritable> value,Context context ) throws IOException, InterruptedException{
		long sum = 0;
		String myValue="";
		String mySum="";
		String myKey = key.toString();
		for (LongWritable val:value){
			sum+= val.get();
			
		}
		myValue=key.toString();
		mySum=String.format("%d", sum);
		myValue=myValue +',' + mySum;
		repToRecordMap.put(new Long( myValue),new Text(myKey));
		if (repToRecordMap.size() > 1) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
	}
	
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			
		for (Text t : repToRecordMap.values()) {
			// Output max amt record to the file system with a null key
			context.write(NullWritable.get(), t);
			}
}
		
	}
public static void main(String[] args) throws Exception {
	
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Top Record for largest amount spent");
    job.setJarByClass(Topbuyer.class);
    job.setMapperClass(TopbuyerMap.class);
    job.setReducerClass(TopbuyerReducer.class);
   job.setMapOutputKeyClass(Text.class);
   job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


