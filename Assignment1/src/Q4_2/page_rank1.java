package Q4_2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class page_rank1 extends Configured implements Tool {
	   public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new page_rank1(), args);
	      
	      System.exit(res);
	   }

	   @Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "Page_Rank");
	      job.setJarByClass(page_rank1.class);
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

	          if (value.charAt(0) != '#') {
	          // ignore comments at the top of the document
	        	  
	        	  String[] both_nodes= value.toString().split("\\t");
	        	  String departure_node = both_nodes[0];
	        	  String arrival_node = both_nodes[1];
	        	  // split nodes
	        	  context.write(new Text(departure_node), new Text(arrival_node));
	        	  // make intermediate output with departure node as key and arrival node as value
	          }
	         
	      }
	      
	   }

	   public static class Reduce extends Reducer<Text, Text, Text, Text> {
	      @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	    	  
	          boolean already_node_in_value = false;
	          
	          String initial_score_with_arrival_nodes = 1 + "\t";
	          // initialize all PR scores at 1
	          
	          for (Text value : values) {
	        	  if (already_node_in_value == true){
	        		  initial_score_with_arrival_nodes += " ; ";
	        	  	  // if-condition prevents separator " ; " from appearing between initial score and arrival_nodes
	        	  }
	        	  
	        	  initial_score_with_arrival_nodes += value.toString();
	        	  already_node_in_value = true;
	        	  
	          }
	           
	          context.write(key, new Text(initial_score_with_arrival_nodes));
	    	  // key = departure node, value = initial score and arrival nodes
	      }
	      
	   }
	   
}
	   

	

