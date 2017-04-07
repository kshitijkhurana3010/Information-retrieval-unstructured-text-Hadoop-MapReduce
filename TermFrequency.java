package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TermFrequency extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TermFrequency.class);
        /* 
        //      The main method invokes ToolRunner, which creates and runs a new instance of DocWordCount, passing the command line arguments. When the application 
        //      is finished, it returns an integer value for the status, which is passed to the System object upon exit.
       */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TermFrequency(), args);
		System.exit(res);
	}
        // The run method configures the job, starts the job, waits for the job to complete, then returns a boolean success flag.
        //setting up a job to run Map and Reduce task
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " Termfrequency ");
		job.setJarByClass(this.getClass());
        // Setting input and output paths
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Map and reduce jobs
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
        // Zero indicates success
		return job.waitForCompletion(true) ? 0 : 1;
	}

	// Mapper class is same as DocWordCount.java mapper class
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();
			Text UpdatedWord = new Text();

			String fileName = context.getInputSplit().toString();
			fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
			fileName = fileName.substring(0, fileName.indexOf(":"));
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				UpdatedWord = new Text(word + "#####" + fileName);
				context.write(UpdatedWord, one);
			}
		}
	}
        //calculating term frequency by reducer step
	public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(word, new DoubleWritable(WordFreq(sum)));
		}
		
		// creating the method to calculate the term-frequency for a current word
		private double WordFreq(int termFreq)
		{
			double result = 0.0;
			if(termFreq>0){
				result = 1 + Math.log10(termFreq);
			}
			return result;
		}
	}
}
