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

public class Genres {
	public static int num;
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private Text genre = new Text();
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line,"::");
	        
	        Configuration conf = context.getConfiguration();
	        String[] movies = conf.get("movies").split("\\,");
	       // String movie = tokenizer.nextToken();
	        // movie = tokenizer.nextToken();
	        word.set(tokenizer.nextToken());
	        word.set(tokenizer.nextToken());
	        int count = 0;
	        while(count < movies.length){
	        	if(word.toString().contains(movies[count])){
	        		String[] genres = tokenizer.nextToken().split("\\|");
	        		int gcnt = 0;
	        		while(gcnt<genres.length){
	        			genre.set(genres[gcnt]);
	        			context.write(genre,one);
	        			gcnt++;
	        		}
	        	}
		        count++;
	        }
	     }
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	       // int sum = 0;
	       // for (IntWritable val : values) {
	         //   sum += val.get();
	       // }
	        context.write(key, new IntWritable());
	        
	    }
	    
}
	 
	
	public static void main(String[] args) throws Exception {
		
		String movies = new String(args[2]);
		
	    Configuration conf = new Configuration();
	    conf.set("movies",movies);
	    Job job = new Job(conf, "genres");
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setJarByClass(Genres.class);
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



