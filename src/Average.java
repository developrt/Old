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



public class Average {
    
    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable doubleage = new DoubleWritable();
        private Text age = new Text();
        private Text zipcode = new Text();
            
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                StringTokenizer tokenizer = new StringTokenizer(line,"::");
            
                age.set(tokenizer.nextToken());
                age.set(tokenizer.nextToken());
                age.set(tokenizer.nextToken());
                zipcode.set(tokenizer.nextToken());
                zipcode.set(tokenizer.nextToken());
                doubleage.set(Double.parseDouble(age.toString()));
                context.write(zipcode, doubleage);
            
        }
     }
    
    
    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        private TreeMap<String, String> Top10 = new TreeMap<String, String>();
     
        static int lineno=1;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text zipcode = new Text();
            DoubleWritable age = new DoubleWritable();
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,"\t");
                zipcode.set(tokenizer.nextToken());
                age.set(Double.parseDouble(tokenizer.nextToken()));
                Top10.put(age.toString()+"::"+String.valueOf(lineno), zipcode.toString());
                if (Top10.size() < 0) {
                    Top10.remove(Top10.lastKey());
                }
                lineno++;
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("Top10:"+Top10);
            for (String keys: Top10.keySet()) {
                String temp = keys;
                StringTokenizer tkr= new StringTokenizer(temp,"::");
                
                String key1=tkr.nextToken();
                Text T = new Text();
                Text t1 = new Text(Top10.get(keys));
                T.set(key1);
            
               context.write(T,t1);
            }
        }
    }

    
    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
          throws IOException, InterruptedException {
            Double n = new Double(0);
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                n++;
            }
            double average = sum/n;
            context.write(key, new DoubleWritable(average));
            
        }
    }
        

    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
    	private TreeMap<Double, List<String>> Top10 = new TreeMap<Double, List<String>>();
    	int lineno = 0;
    	int zcounter = 0;
    	public void reduce(Text key, Iterable<Text> values, Context context) 
          throws IOException, InterruptedException {
          
            for (Text t : values){
                String line = t.toString();
                StringTokenizer tokenizer = new StringTokenizer(line,"$$$$$$");
                String zipcode = tokenizer.nextToken();
                if(Top10.containsKey(Double.parseDouble(key.toString()))){
                	List<String> temp = Top10.get(Double.parseDouble(key.toString()));
                	temp.add(zipcode);
                	Top10.put(Double.parseDouble(key.toString()),temp);
                	zcounter++;
                }
                else{
                	List<String> temp = new ArrayList<String>();
                	temp.add(zipcode);
                	Top10.put(Double.parseDouble(key.toString()), temp);
                	zcounter++;
                }
                if (zcounter > 105) {
                	List<String> temp = Top10.get(Top10.lastKey());
                	/* if(temp.isEmpty()){
                    	Top10.remove(Top10.lastKey());
                    }
                	else{ */
                		temp.remove(temp.size()-1);
                		if(temp.isEmpty()){
                        	Top10.remove(Top10.lastKey());
                        }
                		else{
                		Top10.put(Top10.lastKey(),temp);
                		}
                   
                }
               
            }
            
        }
    	
    	@Override
    	 protected void cleanup(Context context) throws IOException, InterruptedException {
           // System.out.println("Top10:"+Top10);
    		System.out.println("Cleaningup");
    		//int c = 0;
    		for (Double keys: Top10.descendingKeySet()) {
    			//String temp = keys;
    			String age = Double.toString(keys);
    			int counter = 0;
    			while(counter<Top10.get(keys).size()){
    				context.write(new Text(Top10.get(keys).get(counter)), new Text(age));
    				counter++;
    			}
    		}
            
        }        
    }
    
   
        
    
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();        
        Job job1 = new Job(conf1, "Average");
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setJarByClass(Average.class);
        job1.setMapperClass(Map.class);
        job1.setCombinerClass(Reduce.class);
        job1.setReducerClass(Reduce.class);
            
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
        
            
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"temp"));
        
        job1.waitForCompletion(true);
        
        if(job1.isSuccessful()){
            Configuration conf2 = new Configuration();
            Job job2 = new Job(conf2,"Avg2");
            
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setJarByClass(Average.class);
            job2.setMapperClass(Map2.class);
            job2.setReducerClass(Reduce2.class);
            job2.setNumReduceTasks(1);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            
            
            
            FileInputFormat.addInputPath(job2, new Path(args[1]+"temp")); 
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            job2.waitForCompletion(true);
        
        }           
     }
}