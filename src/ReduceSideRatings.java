import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.lang.reflect.Method;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class ReduceSideRatings {

	public static class RatingsMap extends Mapper<LongWritable, Text, Text, Text> {
	
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			InputSplit split = context.getInputSplit();
    	    Class<? extends InputSplit> splitClass = split.getClass();

    	    FileSplit fileSplit = null;
    	    if (splitClass.equals(FileSplit.class)) {
    	        fileSplit = (FileSplit) split;
    	    } else if (splitClass.getName().equals(
    	            "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {

    	        try {
    	            Method getInputSplitMethod = splitClass
    	                    .getDeclaredMethod("getInputSplit");
    	            getInputSplitMethod.setAccessible(true);
    	            fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
    	        } catch (Exception e) {
    	          
    	            throw new IOException(e);
    	        }

    	    }
	    	String filename = fileSplit.getPath().getName();
	    	String [] line = value.toString().split("::");
			
	    	if(filename.contains("users.dat")){
	    		if(line[1].equals("M")){
	    			//System.out.println("users  " + value.toString());
	    			context.write(new Text(line[0]+"U"),new Text("U::"+line[0]));
	    		}
	    	}
	    	
	    	if(filename.contains("movies.dat")){
	    		String[] genres = line[2].split("\\|");
	    	//	System.out.println("movies  " + value.toString());
	    		for(String genre : genres){
	    			if(genre.contains("Action") || genre.contains("Drama")){
	    				// System.out.println("movies  " + value.toString());
	    				context.write(new Text(line[0]+"M"), new Text("M::"+value.toString()));
	    			}
	    		}
	    		
	    	}
	    	
	    	if(filename.contains("ratings.dat")){
	    		//System.out.println(value.toString());
	    		context.write(new Text(line[1]+"R"), new Text("R::" + value.toString()));
	    	}
	    	
		//System.out.println("Completing Map tasks");
		}
		
		
	}
	
	
	public static class RatingsReduce extends Reducer<Text, Text, Text, Text>{
		
		
		//private TreeMap<String, Integer> ratingCount = new TreeMap<String, Integer>();
		private TreeMap<String, String> userMap = new TreeMap<String, String>();
		private TreeMap<String, String> movieMap = new TreeMap<String, String>();
		private List<String> ratingList = new ArrayList<String>();
		
		//private List<String>
	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	    	for(Text val : values){
	    		if(val.toString().charAt(0) == 'U'){
	    			//System.out.println("Reducing users");
	    			userMap.put(key.toString(),val.toString());
	    		}
	    		
	    		if(val.toString().charAt(0) == 'M'){
	    			//System.out.println("Reducing movies");
	    			movieMap.put(key.toString(), val.toString()+"::0.0::0.0");
	    		}
	    		
	    		if(val.toString().charAt(0) == 'R'){
	    			//System.out.println("Reducing ratings");
	    			ratingList.add(val.toString());
	    		}
	    		
	    	}
	    
	    }
	    
	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	    	TreeMap<String,Double> ratingCount = new TreeMap<String,Double>();   
    		System.out.println("Cleaningup");
    		
    		/*for(String keys : movieMap.keySet()){
    			System.out.println(keys);
    		}*/
    		
    		for (String ratings : ratingList){
    			String[] pars = ratings.split("::");
    			String user = pars[1] + "U";
    			//System.out.println(user);
    			String movie = pars[2] + "M";
    			//System.out.println(movie);
    			if(userMap.containsKey(user) && movieMap.containsKey(movie)){
    				String movieInfo = movieMap.get(movie);
    			
    				//System.out.println(movieInfo);
    				String [] params = movieInfo.split("::");
    				Double avg = Double.parseDouble(params[4]);
    				Double count = Double.parseDouble(params[5]);
    				avg = avg + Double.parseDouble(pars[3]);
    				count++;
    				String newMovieInfo = params[0] + "::" +params[1]
    										+ "::" + params[2]
    										+ "::" + params[3]
    												+ "::" +Double.toString(avg) +"::" +Double.toString(count);
    				//System.out.println(newMovieInfo);
    				movieMap.put(movie, newMovieInfo);
    				//ratingCount.put(movieMap.get(movie)),
    			}
    		}
    		DecimalFormat df = new DecimalFormat("0.00");
    		for (String movies : movieMap.keySet()){
    			String movieInfo = movieMap.get(movies);
    			//System.out.println(movieInfo);
    			String [] params = movieInfo.split("::");
    			Double avg = Double.parseDouble(params[4]);
				Double count = Double.parseDouble(params[5]);
				avg = avg/count;
				if(avg >= 4.4 && avg <= 4.7){
					System.out.println("Found Movie");
					context.write(new Text(params[1] +"\t" +params[2] +"\t" +params[3]), new Text(df.format(avg)));
				}
    			
    		}
	    	
	    } 
	    
	}
	
	
	
	public static void main (String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = new Job(conf, "MovieRatings");
	   
	    
	    job.setNumReduceTasks(1);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setJarByClass(RatingsMap.class);
	    job.setMapperClass(RatingsMap.class);
	    job.setReducerClass(RatingsReduce.class);
	   
	   
	    MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, RatingsMap.class);
	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, RatingsMap.class);
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, RatingsMap.class);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	        
	    job.waitForCompletion(true);
	}
	
}
