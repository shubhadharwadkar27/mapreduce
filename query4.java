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



public class query4 {
	public static class MarginMap extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(";");
			String prodid = parts[5];
			double saleamt=Double.parseDouble(parts[8]);
			double cost=Double.parseDouble(parts[7]);
			String qty=parts[6];
			String myvalue= saleamt+ "," +cost+ "," +qty;
			context.write(new Text(prodid), new Text(myvalue));
		}
	}
	public static class MarginReducer extends Reducer<Text,Text,NullWritable,Text>{
		private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();
		public void reduce (Text key,Iterable<Text> value,Context context ) throws IOException, InterruptedException{
			double salesum=0;
			double costsum=0;
			int qtysum=0;
			
			for(Text val : value){
				String[] parts = val.toString().split(",");
				double saleamt=Double.parseDouble(parts[0]);
				double cost=Double.parseDouble(parts[1]);
				int qty=Integer.parseInt(parts[2]);
				salesum += saleamt;
				costsum += cost;
				qtysum += qty;
			}
			double margin=((salesum)-(costsum)/(costsum))*100;
			double profit=(salesum)-(costsum);
			String key1=key.toString();
			// keyS=Long.parseLong(key1);
			String myvalue= key1+ "," +margin+ "," +profit+ "," +qtysum;
			repToRecordMap.put(new Double(margin) ,new Text(myvalue));
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
		Job job = Job.getInstance(conf, "njnqan");
	    job.setJarByClass(query4.class);
	    job.setMapperClass(MarginMap.class);
	    job.setReducerClass(MarginReducer.class);
	   job.setMapOutputKeyClass(Text.class);
	   job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}	
