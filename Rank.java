package org.myorg;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Rank extends Configured implements Tool {

   
   public static void main( String[] args) throws  Exception {
     /* 
     //      The main method invokes ToolRunner, which creates and runs a new instance of DocWordCount, passing the command line arguments. When the application 
     //      is finished, it returns an integer value for the status, which is passed to the System object upon exit.
     */
      int res  = ToolRunner .run( new Rank(), args);
      System .exit(res);
   }
    // The run method configures the job, starts the job, waits for the job to complete, then returns a boolean success flag.
    
   public int run( String[] args) throws  Exception {
    // Create a new instance of job object 
      Job job  = Job .getInstance(getConf(), " Rank ");
   // Set the jar to use
      job.setJarByClass( this .getClass());
      job.setSortComparatorClass(DesendingSort.class);
   // Set the inputs and output paths 
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass(Reduce .class);
      job.setOutputKeyClass(DoubleWritable.class);
      job.setOutputValueClass( Text .class);
   // Zero indicates the success
      return job.waitForCompletion( true)  ? 0 : 1;
   }
   // Read the line, split file name, rank in context and interchange key and values
   public static class Map extends Mapper<LongWritable,Text,DoubleWritable,Text>{
	   private Text word = new Text();
	   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		   String line = value.toString();
		   StringTokenizer tokenizer = new StringTokenizer(line);
		   word.set(tokenizer.nextToken());
		   DoubleWritable number = new DoubleWritable(Double.parseDouble(tokenizer.nextToken()));
		   context.write(number,word);
	   }     
	}
   
   

   
 // Reducer read key value pair and interchange them
   public static class Reduce extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( DoubleWritable score,  Iterable<Text > fileName,  Context context)
         throws IOException,  InterruptedException {

         for (Text file  : fileName) {
        	 context.write(file,  score);
         }
         
      }
   }


   public static class DesendingSort extends WritableComparator {

		//Constructor.
		 
		protected DesendingSort() {
			super(DoubleWritable.class, true);
		}
		
		@SuppressWarnings("rawtypes")

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleWritable k1 = (DoubleWritable)w1;
			DoubleWritable k2 = (DoubleWritable)w2;
			// Desending Sort operation 
			return -1 * k1.compareTo(k2);
		}
	}


}




