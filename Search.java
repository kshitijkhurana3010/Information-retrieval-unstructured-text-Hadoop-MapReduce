package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class Search extends Configured implements Tool {

       private static final Logger LOG = Logger .getLogger( Search.class);
       /* 
       // The main method invokes ToolRunner, which creates and runs a new instance of TermFrequency, passing the command line arguments. When the application 
       // is finished, it returns an integer value for the status, which is passed to the System object upon exit.
       */
       private static final List<String> inputStrings = new ArrayList<String>();
   
       public static void main( String[] args) throws  Exception {

	      System.out.print("Enter the query..: ");
	      Scanner s = new Scanner(System.in);
	      String str = s.nextLine();
	      s.close();
		  
	      inputStrings.addAll(Arrays.asList(str.split(" ")));
	      System.out.println("Data Read...\n"+inputStrings);
              int res  = ToolRunner .run( new Search(), args);
              System .exit(res);
            }
       // The run method configures the job, starts the job, waits for the job to complete, then returns a boolean success flag.
       // 
       public int run( String[] args) throws  Exception {

       String str="";
       for (String sr: inputStrings){
    	    str+= str.equals("")?sr:","+sr;
          }

       Configuration conf = getConf();
       conf.set("istr", str);
       // Create a new instance of job object 
       Job job  = Job .getInstance(conf, " Search ");
       // Set the jar to use
       job.setJarByClass( this .getClass());
       // Set the inputs and output paths 
       FileInputFormat.addInputPaths(job,  args[0]);
       FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
       // Set map and reduce class for the job
       job.setMapperClass( Map .class);
       job.setCombinerClass( Combiner .class);
       job.setReducerClass( Reduce .class);
       job.setOutputKeyClass( Text .class);
       job.setOutputValueClass( Text .class);
       // Zero indicates the success
       return job.waitForCompletion( true)  ? 0 : 1;
        }
      // Map for reading the string the line by line, extracting file name and score
        public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      
        public void map( LongWritable offset,  Text lineText,  Context context)
             throws  IOException,  InterruptedException {

              String line  = lineText.toString();
              String word  = line.substring(0,line.indexOf("#"));
           //
              String fileName = line.substring(line.lastIndexOf("#")+1);
    	      fileName = fileName.substring(0,fileName.indexOf('\t'));
           //
    	      String score = line.substring(line.lastIndexOf("\t")+1);
    	      Configuration conf = context.getConfiguration();
    	      String selStrings = conf.get("istr");
    	      for (String str: Arrays.asList(selStrings.split(","))){
    		  if (word.equalsIgnoreCase(str)){
    			 context.write(new Text(fileName),new Text(score));
    			 break;
    	        	 }
    	         }		 
              }
         }
      // Combiner takes all the scores of that file and saves it a string as value
       public static class Combiner extends Reducer<Text ,  Text ,  Text ,  Text > {
       @Override 
       public void reduce( Text fileName,  Iterable<Text > counts,  Context context)
            throws IOException,  InterruptedException {
         String score = "";
         for (Text count  : counts) {
        	 score  += score.equals("")?count:","+count;
             }
         context.write(new Text(fileName),  new Text(score));
          }
       } 
        //Reducer extract search score for each file name and adding the corresponding scores
         
       public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
	      @Override 
	      public void reduce( Text fileName,  Iterable<Text > counts,  Context context)
	         throws IOException,  InterruptedException {
	         Double score = 0.0;
	         
	         for(Text count: counts){
	        	 String[] tfidfs = count.toString().split(",");
	        	 for (String tfidf: tfidfs){
	        		 score += Double.parseDouble(tfidf);
	        	 }
	         }
	         
	         context.write(new Text(fileName),  new Text(score.toString()));
	      }
	   } 
   

       }
