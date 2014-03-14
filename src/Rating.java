/* Code for Question 1 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Rating {
	public static int num;
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line,"::");
	        //while (tokenizer.hasMoreTokens()) {
	            word.set(tokenizer.nextToken());
	            context.write(word, one);
	        //}
	    }
	 }
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		int n = 0;
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        Configuration conf = context.getConfiguration();
	        if (sum>Integer.parseInt(conf.get("number"))){
	        context.write(key, new IntWritable(sum));
	        }
	    }
	    
}
	 
	
	public static void main(String[] args) throws Exception {
		
		//System.out.println("Enter the number of movies: ");
    	//Scanner scan = new Scanner(System.in);
    	num = Integer.parseInt(args[2]);
	    Configuration conf = new Configuration();
	    conf.setInt("number",num);
	    Job job = new Job(conf, "rating");
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setJarByClass(Rating.class);
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
}



