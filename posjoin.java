package pos;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class posjoin {
public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
        
		
		private Map<String, String> ab = new HashMap<String, String>();
		//private Map<String, String> abMap1 = new HashMap<String, String>();
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		
		  //  Path p1 = new Path(files[1]);
		
			if (p.getName().equals("store_master")) {
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split(",");
						String store_id = tokens[0];
						String state = tokens[2];
						ab.put(store_id,state);
						line = reader.readLine();
					}
					reader.close();
			}
		
if(ab.isEmpty())
{
	throw new IOException("Error");
}
		}

protected void Map(LongWritable key, Text value, Context context) throws Exception
{
	
	
	String row = value.toString();
	String[] tokens = row.split(",");
	String store_id = tokens[0];
	String state = ab.get(store_id);
	//String POS2 = ab.get(state);
	outputKey.set(new Text(row));
	outputValue.set(state);
	//String desig = abMap1.get(emp_id);
	//String ppp = salary + "," + desig; 

	  	context.write(null,null);
} 
}
public static void main(String[] args) throws Exception
{
	Configuration conf =new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job=Job.getInstance(conf);
	job.setJarByClass(posjoin.class);
	job.setJobName("POSJoin");
	job.setMapperClass(MyMapper.class);
	job.addCacheFile(new Path("store_master").toUri());
	job.setNumReduceTasks(0);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	job.waitForCompletion(true);
	
	
	
}
}



