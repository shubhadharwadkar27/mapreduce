import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//query3a

public class viable{
public static class viableMap extends Mapper<LongWritable,Text,Text,Text>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String [] parts=value.toString().split(";");
			String prodsub=parts[4];
			String sales=parts[8];
			String age=parts[2];
			//String cost=parts[7];
			String myValue=age+ "," +sales;
			context.write(new Text(prodsub), new Text(myValue));
			
		
		
		}
}
//Partitioner
public static class viablePartitioner extends Partitioner < Text, Text >
{
   @Override
   public int getPartition(Text key, Text value, int numReduceTasks)
   {
      String[] parts = value.toString().split(",");
      String age = parts[0].trim();
      
      if(age.equals("A"))
    	  {
    	  return 0;
    	  }
       if(age.equals("B")){
    	   return 1;
       }
       if(age.equals("C")) {
    	   return 2;
       }
       if(age.equals("D")) {
    	   return 3;
       }
       if(age.equals("E")) {
    	   return 4;
       }
       if(age.equals("F")){
    	   return 5;
       }
       if(age.equals("G")){
    	   return 6;
       }
       if(age.equals("H")) 
    	   {
    	   return 7;
    	   }
       
       if(age.equals("I")) {
    	   return 8;
       }
       else  return 9;
   }
}
public static class viableReducer extends
Reducer<Text, Text, NullWritable, Text> {
	private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();
	public void reduce (Text key,Iterable<Text> value,Context context ) throws IOException, InterruptedException{
		long salesum=0;
		//long costsum=0;
		String age="";
		//int qtysum=0;
		
		for(Text val : value){
			String[] parts = val.toString().split(",");
			
			salesum=salesum+Long.parseLong(parts[1]);
			
		
			age=parts[0];
		}
		
		String myValue=key.toString();
		
		String total=String.format("%d", salesum);
		 myValue= myValue+ "," +age+ "," +total;
		repToRecordMap.put(new Long(salesum) ,new Text(myValue));
		if (repToRecordMap.size() > 5) {
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
	    job.setJarByClass(viable.class);
	    job.setMapperClass(viableMap.class);
	    job.setPartitionerClass(viablePartitioner.class);
	    job.setReducerClass(viableReducer.class);
	    job.setNumReduceTasks(10);
	   job.setMapOutputKeyClass(Text.class);
	   job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }






}

