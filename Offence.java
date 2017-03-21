import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Offence {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	 
			int speed =Integer.parseInt(str[1]);	            
			
	            context.write(new Text(str[0]),new IntWritable(speed));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,IntWritable,Text,Text>
	   {
		    int offence_percent=0;
		    
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      int offence_count=0;
			int total=0;
				
		         for (IntWritable val : values)
		         {       
				if (val.get()>65)	
		        	offence_count++;
				
				total++;    
		      
			}


		         offence_percent=(offence_count*100)/total;
		String percentvalue=String.format("%d", offence_percent);
String valwithsign = percentvalue + "%";
		      		      
		      context.write(key,new Text (valwithsign) );
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    Job job = Job.getInstance(conf, "Offence Percentage");
		    job.setJarByClass(Offence.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(2);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}