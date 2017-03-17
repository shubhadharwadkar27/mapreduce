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



public class Topproduct{
public static class TopproductMap extends Mapper<LongWritable,Text,Text,LongWritable>{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] parts = value.toString().split(";");
		String prodid = parts[5];
		long amount = Long.parseLong(parts[8]);
		
		context.write(new Text(prodid),new LongWritable(amount));
	}
}
	public static class TopproductReducer extends Reducer<Text,LongWritable,NullWritable,Text>{
		private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();
		public void reduce (Text key,Iterable<LongWritable> value,Context context ) throws IOException, InterruptedException{
				long sum=0;
				String myValue="";
				String mySum="";
				//String myKey = key.toString();
				for (LongWritable val:value){
					sum+= val.get();
				}
				myValue=key.toString();
				mySum=String.format("%d", sum);
				myValue=myValue +',' + mySum;
				repToRecordMap.put(new Long(sum),new Text(myValue));
				if (repToRecordMap.size() > 10) {
							repToRecordMap.remove(repToRecordMap.firstKey());
						}
			}
			
				protected void cleanup(Context context) throws IOException,
				InterruptedException {
					
					for (Text t : repToRecordMap.descendingMap().values()) {
						// Output top records to the file system with a null key
						context.write(NullWritable.get(), t);
						}
		}	
		}
		
		public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Top Record for largest amount spent");
		    job.setJarByClass(Topproduct.class);
		    job.setMapperClass(TopproductMap.class);
		    job.setReducerClass(TopproductReducer.class);
		   job.setMapOutputKeyClass(Text.class);
		   job.setMapOutputValueClass(LongWritable.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
		}	


