package Q4_1;

import java.io.File;
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

public class TF_IDF_3 extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new TF_IDF_3(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "TF_IDF");
      job.setJarByClass(TF_IDF_3.class);
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
       
       String[] word_docid__wordcount_totnbrwords = value.toString().split("\\t");
     // create array of strings where split word_docid from wordcount_totnbrwords
       
       String[] word_docid = word_docid__wordcount_totnbrwords[0].split(" ; ");
       // create array of strings where split word from docid
       String word = word_docid[0];
       String docid = word_docid[1];
       // create different strings for word and docid
       
       String[] wordcount_totnbrwords = word_docid__wordcount_totnbrwords[1].split(" ; ");
      // create array of strings where wordcount from totnbrwords
       String wordcount = wordcount_totnbrwords[0];
       String totnbrwords = wordcount_totnbrwords[1];
      // create different strings for wordcount and totnbrwords
       
       context.write(new Text(word), new Text(docid + " ; " + wordcount + " : " + totnbrwords));
       // key = word \t value = docid + wordcount + totnbrwords
         
      }
   }

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {

          int tot_nbr_documents = new File("/home/cloudera/workspace/Assignment1/input1").listFiles().length;
          // get nbr of documents in input1 folder
          // http://stackoverflow.com/questions/4362888/getting-the-number-of-files-in-a-folder-in-java
          // modify this path when running the file on another machine

          int nbr_docs_where_word = 0;
        
          HashMap<String, String> docid_nbrs_map = new HashMap<String, String>();
        
        for (Text val : values) {
          String[] docid__count_totnbr = val.toString().split(" ; ");
          String docid = docid__count_totnbr[0];
          String count_totnbr = docid__count_totnbr[1];
          // separate docid from count_totnbr
          nbr_docs_where_word++;
          // update nbr of occurences of documents where the word appears
          
          docid_nbrs_map.put(docid, count_totnbr);
          // insert docid and numbers into the hashmap
        }
        
        
        for (String docid_key : docid_nbrs_map.keySet()){
            String[] count__totnbr = docid_nbrs_map.get(docid_key).split(" : ");
            // split count and totnbr
            double count = Double.valueOf(count__totnbr[0]);
            double totnbr = Double.valueOf(count__totnbr[1]);
            // create double variables for count and total nbr of words
            // http://stackoverflow.com/questions/5769669/convert-string-to-double-in-java
            // http://stackoverflow.com/questions/7255078/double-valueofs-vs-double-parsedouble
            
            double tf = count / totnbr ;
            // compute tf term frequency
            double idf = Math.log( (double) tot_nbr_documents / (double) nbr_docs_where_word );
            // compute idf inverse document frequency
            double tf_idf = tf * idf;
            
            context.write(new Text(key + " ; " + docid_key), 
              new Text(String.valueOf(tf_idf))
          // http://stackoverflow.com/questions/5766318/converting-double-to-string
          );
            
        }

        }

      }

}

