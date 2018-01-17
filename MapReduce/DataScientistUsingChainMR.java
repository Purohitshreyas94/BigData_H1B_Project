import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DataScientistUsingChainMR {

	public static class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			String job_title = record[4];
			String case_status = record[1];
			String soc_name = record[3];
			
			if(job_title.contains("DATA SCIENTIST") && case_status.equals("CERTIFIED"))
			{
				context.write(new Text(soc_name), one);
			}
			
		}
		
	}
	
	public static class FirstReducer extends Reducer<Text, IntWritable, LongWritable, Text>
	{	
		
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException 
		{
			long sum =0;
			for(IntWritable val : values)
			{
				sum += val.get();
			}
			
			//String myValue = key.toString() +","+sum;
			
			context.write(new LongWritable(sum), new Text(key));
			
			
		}
	}
	
	public static class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException 
		{
			String[] token = value.toString().split("\t");
			long myKey = Long.parseLong(token[0]);
			String myValue = token[1];
			
			context.write(new LongWritable(myKey), new Text(myValue));
		}
	}
	
	public static class SortReducer extends Reducer<LongWritable, Text, Text, LongWritable>
	{
		int count = 0;
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			if(count < 5)
			{
				for(Text val : values)
				{
					context.write(new Text(val),key);
					count++;
				}
				
			}
		}
		
	}
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf,"Using chain Map Reduce for Finding the Most Data Scientist Job");
		
		job.setJarByClass(DataScientistUsingChainMR.class);
		
		job.setMapperClass(FirstMapper.class);
		job.setReducerClass(FirstReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		Path outputpath1 = new Path("FirstMapper2");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputpath1);
		
		FileSystem.get(conf).delete(outputpath1, true);
		job.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf, "Using Chain Map Reduce For Finding the Data Scientis Job");
		
		job2.setJarByClass(DataScientistUsingChainMR.class);
		
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);
		
		job2.setSortComparatorClass(DecreasingComparator.class);
		
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job2, outputpath1);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		FileSystem.get(conf).delete(new Path(args[1]), true);
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}

}
