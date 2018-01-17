import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Q3_ChainMapReduce {
	
	
	//Mapper Class
	public static class MapperClass extends Mapper <LongWritable, Text, Text, LongWritable>
	{

		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			
			String[] str = value.toString().split("\t");
			String soc_name = str[3];
			String job = str[4];
			String status = str[1];
		    if (job.contains("DATA SCIENTIST") && status.equals("CERTIFIED"))
		    {
		    	context.write(new Text(soc_name), new LongWritable(1));
		    }
		    
		    
		}
	}
	
	
	
	 //Reducer Class
	public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable>
	{
		int max = 0;
		String opstr="";
		int counter = 0;
         
	  protected void reduce(Text key, Iterable<LongWritable> values, Context context)throws IOException, InterruptedException 
	  {
			int count =0;
			
			for(LongWritable val:values)
			{
				count+=val.get();
				
				
			}
			
		/*	if(max < count)
			{
				max = count;
				opstr = key.toString();
			}
			
		*/	
			context.write(new Text(key), new LongWritable(count));
			
		}
	  
   }
	
	
	//Mapper2
	public static class Mapper2 extends Mapper<LongWritable,Text,Text,LongWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valueArr = value.toString().split("\t");
			context.write(new Text(valueArr[0]), new LongWritable(Long.parseLong(valueArr[1])));
		}
	}

	
	
	//Reducer2
	public static class Reducer2 extends Reducer<Text,LongWritable,LongWritable,Text>
	{
		@Override
		public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException
		{
			long tot = 0;
			for(LongWritable val : value)
			{
				tot+=val.get();
			}
			context.write(new LongWritable(tot), key);
		}
	}
	

	
	//Sort Mapper
  public static class SortMapper extends Mapper<LongWritable,Text,LongWritable,Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valueArr = value.toString().split("\t");
			context.write(new LongWritable(Long.parseLong(valueArr[0])), new Text(valueArr[1]));
		}
	}


	
  //Sort Reducer
  public static class SortReducer extends Reducer<LongWritable,Text,Text,LongWritable>
	{
	    int counter = 0;
		public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			for(Text val : value)
			{
				if(counter < 1)
				{
					context.write(new Text(val), key);
					counter= counter+1;
				}
				
			}
		}
	}
	 
	 
	 
	
    //Main Method
	public static void main(String[] args)throws Exception
	{
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Industry with the most number of Data Scientist Positions");
	    job1.setJarByClass(Q3_ChainMapReduce.class);
	    job1.setMapperClass(MapperClass.class);
	   job1.setReducerClass(ReducerClass.class);
	  //   job.setNumReduceTasks(0);
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(LongWritable.class);
	    job1.setOutputKeyClass(Text.class);
	   job1.setOutputValueClass(LongWritable.class);
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    Path outputPath1 = new Path("Psotion1");
		FileOutputFormat.setOutputPath(job1, outputPath1);
		job1.waitForCompletion(true);
	    
	    
	    Job job2 = Job.getInstance(conf, "Total Sales - Product ID");
	    job2.setJarByClass(Q3_ChainMapReduce.class);
	    job2.setMapperClass(Mapper2.class);
	    job2.setReducerClass(Reducer2.class);
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(LongWritable.class);
	    job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(Text.class);
		Path outputPath2 = new Path("Postion2");
		FileInputFormat.addInputPath(job2,outputPath1);
		//FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job2, outputPath2);
	    job2.waitForCompletion(true);
	   
	   
	    
	   Job job3 = Job.getInstance(conf,"Per Product Id - totalSales");
		job3.setJarByClass(Q3_ChainMapReduce.class);
		job3.setMapperClass(SortMapper.class);
		job3.setReducerClass(SortReducer.class);
		job3.setSortComparatorClass(DecreasingComparator.class);
		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job3, outputPath2);
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	    
	    
	}
	
	
	
}