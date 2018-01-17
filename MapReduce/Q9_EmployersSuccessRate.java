
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q9_EmployersSuccessRate 
{
  
	public static class MapperClass extends Mapper <LongWritable, Text, Text, Text>
	{

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String str[] =value.toString().split("\t");
			String status = str[1];
			String Employer =str[2];
			context.write(new Text(Employer), new Text(status));
		}
		
	}
	
	
	public static class ReducerClass extends Reducer <Text,Text,LongWritable,Text>
	{

		//private TreeMap<Double, String> topten = new TreeMap<Double, String>();
		public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException
		{
			 long total =0;
			 long certified = 0;
			 
			 for (Text val:value)
			 {
				 String status = val.toString();
				 
				 total++;
				 
				 if(status.equals("CERTIFIED") || status.equals("CERTIFIED-WITHDRAWN")) 
				 {
					certified ++ ;
				    
				 }
				  
				    
				
				    
			 }
			 String myKey = key.toString();
			 long success = (certified);
			long rate = (success*100)/total;
			if(rate >=70 && total >=1000)
			{
				//String op = key.toString()+ ","+String.format("%f",total)+"," + String.format("%f",rate);
				String output =myKey+"\t"+total;
			
			
				
				context.write(new LongWritable(rate), new Text(output));
	
			}
	}
}		
		//Sort Mapper
		  public static class SortMapper extends Mapper<LongWritable,Text,LongWritable,Text>
			{
				public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
				{
                   
					String[] token = value.toString().split("\t");
					long myKey = Long.parseLong(token[0]);
					String  total= token[2];
					String emp = token[1];
					String myValue  = total+","+emp;
					context.write(new LongWritable(myKey), new Text(myValue));					
				   
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
						if(counter < 10)
						{	
							context.write(new Text(val), key);
							counter = counter+1;
						}
					}
				}
			}
		
		
		  public static void main(String[] args) throws Exception {
				
				Configuration conf =new Configuration();
				//conf.set("mapreduce.output.textoutputformat.separator", ",");
				
				Job job=Job.getInstance(conf,"Petitions with the success rate more than 70%");
				job.setJarByClass(Q9_EmployersSuccessRate.class);
				job.setMapperClass(MapperClass.class);
				job.setReducerClass(ReducerClass.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(LongWritable.class);
				job.setOutputValueClass(Text.class);
				
				Path outputpath1 = new Path("Successrate1");
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, outputpath1);
				FileSystem.get(conf).delete(outputpath1, true);
				job.waitForCompletion(true);
				
				
				
				Job job2 = Job.getInstance(conf,"Petitions with the success rate more than 70%");
				job2.setJarByClass(Q9_EmployersSuccessRate.class);
				job2.setMapperClass(SortMapper.class);
				job2.setSortComparatorClass(DecreasingComparator.class);
				job2.setReducerClass(SortReducer.class);
			    job2.setMapOutputKeyClass(LongWritable.class);
			    job2.setMapOutputValueClass(Text.class);
			    job2.setOutputKeyClass(Text.class);
			    job2.setOutputValueClass(LongWritable.class);
			    
			    FileInputFormat.addInputPath(job2, outputpath1);
				FileOutputFormat.setOutputPath(job2, new Path(args[1]));
				
				System.exit(job2.waitForCompletion(true) ? 0 : 1);

			}
		
  }