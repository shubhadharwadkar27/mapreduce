import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class SentimentAnalysis {
public static class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private Map<String, String> mapab=new HashMap<String,String>();
	//private Text word=new Text();
	String myword="";
	int myvalue=0;
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		URI[] files=context.getCacheFiles();
		Path p=new Path(files[0]);
		
		if(p.getName().equals("AFINN.txt"))
		{
			BufferedReader reader=new BufferedReader(new FileReader(p.toString()));
			String line=reader.readLine();
			while(line!=null)
			{
				String []parts=line.split("\t");
			//	String dict_word=parts[0];
			//	String dict_value=parts[1];
				mapab.put(parts[0],parts[1]);
				line=reader.readLine();
				
			}
			reader.close();
		}
		if(mapab.isEmpty())
		{
			throw new IOException("Error: Unable to load data");
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		StringTokenizer itr=new StringTokenizer(value.toString());
		while(itr.hasMoreTokens()){
			myword=itr.nextToken().toLowerCase();
			if(mapab.get(myword)!= null)
			{
				myvalue=Integer.parseInt(mapab.get(myword));
				if(myvalue>0)
				{
					myword="positive";
				}
				else if(myvalue<0)
				{
					myword="negative";
					myvalue=myvalue*-1;
				}
				
			}
			else 
			{
				myword="positive";
				myvalue=0;
			}
			//word.set(myword);
			
		
		//System.out.println(myvalue);
		context.write(new Text(myword), new IntWritable(myvalue));
	}
}
}
public static class SentimentReducer extends Reducer<Text, IntWritable, NullWritable, Text>
{
	int pos=0;
	int neg=0;
	double percent=0.00;
	String str="";
	public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException
	{
		int sum=0;
		for(IntWritable val:value)
		{
			sum=sum+val.get();
		}
		if(key.toString().equals("positive"))
			pos=sum;
		else if (key.toString().equals("negative"))
		neg=sum;
		//String sum1 = String.format("%d", sum);
		//context.write(NullWritable.get(),new Text(sum1));
		//percent=(pos-neg)/(pos+neg);
		
	}

protected void cleanup(Context context) throws IOException, InterruptedException
{
	percent=((double)pos-neg)/(double)(pos+neg);
	String str=String.format("%f",percent);
	context.write(NullWritable.get(),new Text(str));
}
}
	public static void main(String[] args) throws Exception
	{
		Configuration conf =new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job=Job.getInstance(conf);
		job.setJarByClass(SentimentAnalysis.class);
		job.setJobName("POSJoin");
		job.setMapperClass(SentimentMapper.class);
		job.addCacheFile(new Path("AFINN.txt").toUri());
		//job.setNumReduceTasks(0);
		job.setReducerClass(SentimentReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
		
	}
}

