import java.io.IOException;
import java.nio.ByteBuffer;
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
                Top10.put(age.get()+"::"+String.valueOf(lineno), zipcode.toString());
                if (Top10.size() > 10) {
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
                
                String key1=tkr.nextToken();//System.out.println("check:"+temp);
                Text T = new Text();
                Text t1 = new Text(Top10.get(keys));
                T.set(key1);
            //    System.out.println(t1);
               context.write(T,t1);
            }
            //System.out.println("This is final text: \n" +temp);
            
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
        //private TreeMap<String, String> tempTop10 = new TreeMap();
        public void reduce(Text key, Iterable<Text> values, Context context) 
          throws IOException, InterruptedException {
            
            //String zg = new String();
            for (Text t : values){
            //    System.out.println(t.toString());
                String line = t.toString();
                StringTokenizer tokenizer = new StringTokenizer(line,"$$$$$$");
                String zipcode = tokenizer.nextToken();
                //String age = 
                //StringTokenizer str= new StringTokenizer(age,"::");
                
                //System.out.println(age);
                System.out.println(zipcode);
                //tempTop10.put(key.toString(), zipcode);
                context.write(new Text(zipcode), new Text(key.toString()));
            }
            /*if(tempTop10.size()>10){
                tempTop10.remove(tempTop10.firstKey());
            }
            String output = new String(); 
            for (Double keys: tempTop10.descendingKeySet()) {  
                 output = output + "\n" +  tempTop10.get(keys) + Double.toString(keys);
            }*/
        //    context.write(new Text(tempTop10.get(keys)), new Text(output));
        }
                    
    }
    
    public static class IntComparator extends WritableComparator {

        public IntComparator() {
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {

            Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
            Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

            return v1.compareTo(v2) * (-1);
        }
    }
        
    
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();        
        Job job1 = new Job(conf1, "Average");
        
        //job1.setMapOutputKeyClass(Text.class);
        //job1.setMapOutputValueClass(DoubleWritable.class);
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
            Job job2 = new Job(conf2,"Avg");
            
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setJarByClass(Average.class);
            job2.setMapperClass(Map2.class);
    //        job2.setCombinerClass(Reduce2.class);
            job2.setReducerClass(Reduce2.class);
            job2.setNumReduceTasks(1);
            //job2.setSortComparatorClass(revComparator.class);
                
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            
            
            
            FileInputFormat.addInputPath(job2, new Path(args[1]+"temp")); 
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            job2.waitForCompletion(true);
        
        }           
     }
}