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
//new query2

public class orders_sales {
	public static class OrderMap extends Mapper<LongWritable,Text,Text,LongWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(";");
			long sales = Long.parseLong(parts[8]);
			String prodid=parts[5];
			context.write(new Text(prodid), new LongWritable(sales));
		}
	
	}
	public static class OrderReducer extends Reducer<Text,Text,NullWritable,Text>{
		private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();
		
			public void reduce(NullWritable key, Iterable<Text> values,
					Context context) throws IOException, InterruptedException {
					long sum=0;
				for (Text value : values) {
						String record = value.toString();
						String[] parts = record.split(",");
						long cost = Long.parseLong(parts[8]);
						sum +=cost;
						
						repToRecordMap.put(sum, new Text(value));
					if (repToRecordMap.size() > 1) {
								repToRecordMap.remove(repToRecordMap.firstKey());
							}
						}
					for (Text t : repToRecordMap.descendingMap().values()) {
						// Output top records to the file system with a null key
						context.write(NullWritable.get(), t);
						}
			}
		
		}
public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top Record for largest amoubt spent");
	    job.setJarByClass(orders_sales.class);
	    job.setMapperClass(OrderMap.class);
	    job.setReducerClass(OrderReducer.class);
	    //job.setNumReduceTasks(0);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}

