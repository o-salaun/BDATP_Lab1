package Q4_1;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TF_IDF_2 extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new TF_IDF_2(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "TF_IDF");
      job.setJarByClass(TF_IDF_2.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      FileSystem fs = FileSystem.newInstance(getConf());
      if (fs.exists(new Path(args[1]))) {
    	  fs.delete(new Path(args[1]), true);
      }
      job.waitForCompletion(true);
      
      return 0;
   }
   
   
   
   
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	 
    	 String[] word_docid__wordcount = value.toString().split("\\t");
		 // create array of strings where split word_docid from wordcount
    	 String[] word_docid = word_docid__wordcount[0].split(" ; ");
    	 // create array of strings where split word from docid
    	 String word = word_docid[0];
    	 String docid = word_docid[1];
    	 // create different strings for word and docid
    	 
    	 context.write(new Text(docid), new Text(word + "=>" + word_docid__wordcount[1]));
    	 // docid (tab) word => wordcount
         
      }
   }

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	  
    	  int total_nbr_words_in_docid = 0;
    	  HashMap<String, Integer> word_count_map = new HashMap<String, Integer>();
    	  
    	  for (Text val : values) {
    		  String[] word_count = val.toString().split("=>");
    		  String word = word_count[0];
    		  int count = Integer.parseInt(word_count[1]);
    		  // http://stackoverflow.com/questions/5585779/how-to-convert-a-string-to-an-int-in-java
    		  
    		  word_count_map.put(word, count); // include word and count into map
    		  // http://stackoverflow.com/questions/20043824/how-to-put-values-into-hashmapint-liststring
    		  total_nbr_words_in_docid += count; // update total nbr of words
    	  }
    	  
    	  for (String word : word_count_map.keySet()){
    		  context.write(new Text(word + " ; " + key),
    				  // set key as word + docid
    				  new Text(word_count_map.get(word) + " ; " + total_nbr_words_in_docid )
    		  		  // set value as count + total nbr of words in docid
    		  );
    	  }

      }
   }

}