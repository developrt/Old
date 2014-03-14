import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class MapsideTop10 {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	   // private final static IntWritable one = new IntWritable(1);
	    private String userid;
	    private String movieid;
	    private TreeMap<Integer,String> userinfo = new TreeMap<Integer,String>();
	    private TreeMap<Integer, List<String>> NewTop10 = new TreeMap<Integer, List<String>>();
	   
	    
	    public void setup(Context context) throws IOException,
		InterruptedException {
		  
		     
			Path[] files =	DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path p : files) {
				Scanner s = new Scanner(new File(p.toString()));
				while(s.hasNext()){
					String line = s.nextLine();
					if (!line.isEmpty()){
						 String [] itr = line.split("::");  
							userinfo.put(Integer.parseInt(itr[0]), line);
						
					}
					
				}
			}
			
		}
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line,"::");
	        
	            userid = tokenizer.nextToken().toString();
	            movieid =tokenizer.nextToken().toString();
	            
	            if(!NewTop10.containsKey(Integer.parseInt(userid))){
	            	List<String> movielist = new ArrayList<String>();
	            	movielist.add(movieid);
	            	NewTop10.put(Integer.parseInt(userid),movielist);
	            }
	            else{
	            	List<String> movielist = NewTop10.get(Integer.parseInt(userid));
	            	movielist.add(movieid);
	            	NewTop10.put(Integer.parseInt(userid),movielist);
	            }
	            
	    }
	    
	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	   
    		for (Integer keys: NewTop10.keySet()){
    			  List<String> movielist = new ArrayList<String>();
    			  movielist = NewTop10.get(keys);
    			  for (String movies : movielist){
    				  String line = userinfo.get(keys) + "::" + movies;
    				  //System.out.println(Integer.toString(keys) + "   " +line);
    				  context.write(new Text(Integer.toString(keys)), new Text(line));
    			  }
    			}
    		}
	 }
	
	 
	 
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		private TreeMap<Integer, List<String>> NewTop10 = new TreeMap<Integer, List<String>>();
		
		
	    public void reduce(Text key, Iterable<Text> values, Context context)
	      throws IOException, InterruptedException {
	       
	    	int sum = 0;
	        String line = "";
	        for (Text val : values) {
	          
	        	line = val.toString();
	        	sum++;
	        }
	      
	        if(!NewTop10.containsKey(sum)){
	        	List<String> temp = new ArrayList<String>();
	        	temp.add(line);
	        	 NewTop10.put(sum,temp);
	        }
	        else{
	        	List<String> temp = NewTop10.get(sum);
	        	temp.add(line);
	        }
	       
	    }
	    

    	@Override
    	protected void cleanup(Context context) throws IOException, InterruptedException {
    		
    		int counter = 0;
    		//System.out.println("Size of the TreeMap: " +NewTop10.size());
    		String msg = "UserId " + "Age " + "Gender " +"\tCount";
    		context.write(new Text(msg), new Text(""));
    		for (Integer keys: NewTop10.descendingKeySet()) {
    			
    				List<String> ops = NewTop10.get(keys);
    				for (String line : ops){
    					String[] fop = line.split("::");
    					String lines = fop[0] + " " + fop[2] + "\t\t" + fop[1] + " ";
        				context.write(new Text(lines), new Text(Integer.toString(keys)));
        				counter++;
    				}
    				
    				if (counter == 10){
    					break;
    				}
    			}
    		}
            
 } 

	public static void main(String[] args) throws Exception {
			
		    Configuration conf = new Configuration();
		    String Path = "/Spring2014_HW-1/input_HW-1";
		    DistributedCache.addCacheFile(new URI(Path + "/users.dat"), conf);
		    Job job = new Job(conf, "NewTop10");
		   
		  
		    
		    job.setNumReduceTasks(1);
		   
		   // DistributedCache.addCacheFile(new URI(Path), job.getConfiguration());
		    
		    URI[] lfs = DistributedCache.getCacheFiles(job.getConfiguration());
		    
		   System.out.println(lfs[0]);
		    System.out.println("File added to cache successfully");
		   // job.setMapOutputKeyClass(Text.class);
		   // job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    job.setJarByClass(Map.class);
		    job.setMapperClass(Map.class);
		    //job.setCombinerClass(Reduce.class);
		    job.setReducerClass(Reduce.class);
		        
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		   
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		        
		    job.waitForCompletion(true);
		    
		 }

}
