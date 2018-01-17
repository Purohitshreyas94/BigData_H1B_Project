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



public class Q2A_MostDataEngJobs 
{
	
	//Mapper Class
  public static class MapperClass extends Mapper<LongWritable, Text, Text, Text>
  {
	  

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String mySearchText = context.getConfiguration().get("myText");
		
		String[] str = value.toString().split("\t");
		String case_status = str[1];
		String job_title = str[4];
		String year = str[7];
		String worksite = str[8];
		
		if(mySearchText.equals("ALL"))
		{
		
		  if(case_status.equals("CERTIFIED") && job_title.contains("DATA ENGINEER")) 
		  {
			context.write(new Text(worksite), new Text(year));
		  }
	   }
		
		else
		{

			  if(case_status.equals("CERTIFIED") && job_title.contains("DATA ENGINEER") && year.equals(mySearchText))
			  {
				context.write(new Text(worksite), new Text(year));
			  }
		}
	}
  
}
  
  
  //Partitioner Class
  public static class PartitionerClass extends Partitioner <Text,Text>
  {

	@Override
	public int getPartition(Text key, Text value, int numReduceTask) 
	{
		String year = value.toString();
		
		if(year.equals("2011"))
		{
			return 0 % numReduceTask;
		}
		
		if(year.equals("2012"))
		{
			return 1 % numReduceTask;
		}
	
		if(year.equals("2013"))
		{
			return 2 % numReduceTask;
		}
	
		if(year.equals("2014"))
		{
			return 3 % numReduceTask;
		}
		
		if(year.equals("2015"))
		{
			return 4 % numReduceTask;
		}
		else
		{
			return 5 % numReduceTask;
		}
	
	}
	  
  }
  
  
  //Reducer Class
  public static class ReducerClass extends Reducer<Text, Text, NullWritable, Text>
  {

	  TreeMap<Integer, Text> map = new TreeMap<Integer, Text>();
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
	{
		int count = 0;
		String year = "";
		for(Text val : values)
		{
			year = val.toString();
			count ++;
		}
		
		String myKey = key.toString();
		
		String myValue = year+","+myKey+","+count;
		
		map.put(new Integer(count), new Text(myValue));
		
		if(map.size() > 1)
		{
			map.remove(map.firstKey());
		}
		
	}
	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException 
	{
		for(Text top5 : map.descendingMap().values())
		{
			context.write(NullWritable.get(), top5);
		}	
	}
	
	
}
  
  
   //Main Method
  public static void main(String[] args) throws Exception
  {
	  Configuration conf = new Configuration();
	  
	  if(args.length > 2)
	  {
		  
		   conf.set("myText", args[2]);  
	  }
	  
	  Job job = Job.getInstance(conf, "Part of the US has the most Data Engineer jobs for each year");
	  job.setJarByClass(Q2A_MostDataEngJobs.class);
	  
	  job.setMapperClass(MapperClass.class);
	  
	  if(args[2].equals("ALL"))
	  {
	     job.setPartitionerClass(PartitionerClass.class);
	     job.setNumReduceTasks(6);
	  }
	  
	  job.setReducerClass(ReducerClass.class);
	  
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(Text.class);
	  
	  job.setOutputKeyClass(NullWritable.class);
	  job.setOutputValueClass(Text.class);
	  
	  FileInputFormat.addInputPath(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
  }
  
  
 }