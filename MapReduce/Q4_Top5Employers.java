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


public class Q4_Top5Employers {

	//Mapper Class
	public static class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		LongWritable one = new LongWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String mySearchText = context.getConfiguration().get("myText");
			
			String[] record = value.toString().split("\t");
			String year = record[7];
			String employee_name = record[2];
			
			String myVal = year+ ","+one;
			
			if(mySearchText.equals("ALL"))
			{
			     context.write(new Text(employee_name), new Text(myVal));
			}
			
			else 
			{
				if(year.equals(mySearchText))
				{
					context.write(new Text(employee_name), new Text(myVal));
				}
			}
			
			
			}
		
	}
	
	
	 //Partitioner Class
	public static class YearPartitioner extends Partitioner<Text, Text>
	{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
		
			String[] str = value.toString().split(",");
			
			String year = str[0];
			
			if(year.equals("2011"))
			{
				return 0 % numReduceTasks;
			}
			else if(year.equals("2012"))
			{
				return 1 % numReduceTasks;
			}
			else if(year.equals("2013"))
			{
				return 2 % numReduceTasks;
			}
			else if(year.equals("2014"))
			{
				return 3 % numReduceTasks;
			}
			else if(year.equals("2015"))
			{
				return 4 % numReduceTasks;
			}
			else
			{
				return 5 % numReduceTasks;
			}
		}
		
	}
	
	 
	//Reducer Class
	public static class EmployeeReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		TreeMap<Integer, Text> map = new TreeMap<Integer,Text>();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			int sum = 0;
			String year = "";
			for(Text val : values)
			{
				String[] token = val.toString().split(",");
				year = token[0];
				int count = Integer.parseInt(token[1]);
				
				sum += count;
			}
			
			String employee_name = key.toString();
			String myValue = year+ ","+employee_name + "," + sum;
			
			map.put(new Integer(sum), new Text(myValue));
			if(map.size() > 5)
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
	public static void main(String[] args) throws Exception {
		
		Configuration conf =  new Configuration();
		
		if(args.length > 2)
		  {
			  
			   conf.set("myText", args[2]);  
		  }
		
		Job job = Job.getInstance(conf, "Top 5 Employers file the most Petitions for each Year");
		
		job.setJarByClass(Q4_Top5Employers.class);
		
		job.setMapperClass(EmployeeMapper.class);
		
		
		if(args[2].equals("ALL"))
		  {
		  
		     job.setPartitionerClass(YearPartitioner.class);
		     job.setNumReduceTasks(6);
		  }
		
		
		job.setReducerClass(EmployeeReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
