import java.io.IOException;
import java.util.*;
//
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
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
	    private Text movie = new Text();
	    private Text genre = new Text();
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] tokenizer = line.split("::");
	        Configuration conf = context.getConfiguration();
	        String movies = conf.get("movies");
	        movie.set(tokenizer[1]);
	        int count = 0;
	        	if(movies.contains(movie.toString())){
	        		String[] genres = tokenizer[2].split("\\|");
	        		int gcnt = 0;
	        		while(gcnt<genres.length){
	        			genre.set(genres[gcnt]);
	        			context.write(NullWritable.get(),genre);
	        			gcnt++;
	        		}
	        	}
	     }
	}
	
	public static class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> {
	    private Set <String> gset = new TreeSet<String>();
		public void reduce(NullWritable key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	       for (Text value : values) {
	    	   gset.add(value.toString());
	       }
	       Text fgenre = new Text();
	       Iterator<String> genre = gset.iterator();
	       String tempgenres = new String();
	       while(genre.hasNext()){
	    	   tempgenres = tempgenres + genre.next() + "," ;
	       }
	      int count = 0;
	      String genres = new String(); 
	       while (count<tempgenres.length()-1){
	    	   genres = genres + tempgenres.charAt(count);
	    	   count++;
	       }
	       fgenre.set(genres);
    	   context.write(NullWritable.get(),fgenre);
	    }
	    
}
	 
	
	public static void main(String[] args) throws Exception {
		
		String movies = new String(args[2]);
		
	    Configuration conf = new Configuration();
	    conf.set("movies",movies);
	    Job job = new Job(conf, "Genres");
	    
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
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



