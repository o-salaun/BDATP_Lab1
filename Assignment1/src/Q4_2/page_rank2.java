package Q4_2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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


public class page_rank2 extends Configured implements Tool {
	   public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new page_rank2(), args);
	      
	      System.exit(res);
	   }

	   @Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "Page_Rank");
	      job.setJarByClass(page_rank2.class);
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


	        	  String[] FromNode_score_ToNodes = value.toString().split("\\t");
	        	  String FromNode = FromNode_score_ToNodes[0];
	        	  String PR_score = FromNode_score_ToNodes[1];
	        	  String ToNodes = FromNode_score_ToNodes[2];
	        	  // split the three elements of intermediary output
	        	  
	        	  String[] ToNodes_array = ToNodes.split(" ; ");
	        	  String separator = "\t";
	        	  // split arrival nodes for each departure node
	        	  
	        	  for (String ToNode_str : ToNodes_array){
	        		  Text PR_score_and_nbr_ToNodes = new Text(PR_score + separator + ToNodes_array.length);
	        		  context.write(new Text(ToNode_str), new Text(PR_score_and_nbr_ToNodes));
	        		  // create separate output for capturing initial PR and number of destination nodes
	        	  }

	        	  context.write(new Text(FromNode), new Text("*" + ToNodes));
	        	  // key = departure_node \t value = list of destination nodes
	         
	      }
	   }

	   public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
	      @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	    	  
	    	  String target_nodes = "";
	    	  double ratio_PR_nbrlinks = 0.0;
	          
	          for (Text value : values) {

	        	  String value_str = value.toString();

	        	  if (value_str.startsWith("*")) {

	        		  target_nodes += value_str.substring("*".length());
	        		  // for each key, arrival nodes are stored in string target_nodes
	        		  // so that they can be used for future iteration if needed
	        		  
	        	  }else{

	        		  String[] PR_and_ToNodes = value_str.split("\\t");
	        		  double initial_score_PR = Double.parseDouble(PR_and_ToNodes[0]);
	        		  int nbr_ToNodes = Integer.parseInt(PR_and_ToNodes[1]);
	        		  // extract initial page rank score and number of destination nodes
	        		  
	        		  ratio_PR_nbrlinks += (initial_score_PR / nbr_ToNodes) ;
	        		  // compute ratio of page rank score over the number of arrival nodes
	        		  
	        	  }
	          }
              
	      double score_PR = ( ( (1 - 0.85) / 508837) + (0.85 * ratio_PR_nbrlinks) ) ;
	      // compute page rank score
	      // PR = (1 - damping factor) / total number of departure nodes) +
	      //					(damping factor * ratio PR over number of arrival nodes)
	    
	      context.write(key, new DoubleWritable(score_PR)); 
	      // output: key = departure node, value = page rank score
	      
	      //String separator = "\t" ;
	      //context.write(key, new Text(score_PR + separator + target_nodes));
	      // for adding the target nodes of each key, uncomment the two lines
	      // above and add Text as value output type
	      
	      }

	  }
}
	   

	

