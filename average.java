import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//query9
public class average {
	public static class averageMap extends Mapper<LongWritable,Text,Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String parts[]=value.toString().split(";");
			String prodid=parts[5];
			String cost=parts[7];
			String qty=parts[6];
			String myvalue= cost+ "," +qty;
			context.write(new Text(prodid), new Text(myvalue));	
		}
	}
	public static class averageReducer extends Reducer<Text, Text,Text, Text >
	{
	//	private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();
		public void reducer(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			long totalcost=0;
			double totalqty=0;
			double avg=0;
			float cost=0;
			int qty=0;
		//	String prodid="";
			
				for(Text val:value){
					
				String parts[]=val.toString().split(",");
				
				 cost=Long.parseLong(parts[0]);
				 qty=Integer.parseInt(parts[1]);
				totalcost += cost;
				totalqty += qty;
			
			}
			avg= (double)totalcost/(double)totalqty;
			//String myvalue=key.toString();
			String myavg=String.format("%f", avg);
			String mycost=String.format("%d",totalcost);
			String myqty=String.format("%d",totalqty);
			String myvalue=myqty+ ',' +mycost+ ',' +myavg;
			
			
			context.write(key,new Text(myvalue));
		}
	}
public static void main(String[] args) throws Exception {
		
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Average");
    job.setJarByClass(average.class);
    job.setMapperClass(averageMap.class);
   
    job.setReducerClass(averageReducer.class);
   
   job.setMapOutputKeyClass(Text.class);
   job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
	
