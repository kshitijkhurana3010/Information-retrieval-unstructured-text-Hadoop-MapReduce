package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);
	private static final TermFrequency termfreq = new TermFrequency();

	public static void main(String[] args) throws Exception {
        // For execution of Job1 Term Frequency
		ToolRunner.run(termfreq,args);
		int res = ToolRunner.run(new TFIDF(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		int result = 0;
		//to get the number of files in the input folder
		FileSystem numberOfFile = FileSystem.get(termfreq.getConf());
		FileStatus[] status = numberOfFile.listStatus(new Path(args[0]));
		Configuration conf = new Configuration();
		conf.set("name", Integer.toString(status.length));

		//Map and reduce jobs for second job
		Job job2 = Job.getInstance(conf, " TFIDF ");
		job2.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job2, args[1]);
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		result = job2.waitForCompletion(true) ? 0 : 1;
		
		return result;
	}


	//Map class for the second job
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text word = new Text();

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();

			//Modifying the text of the line to generate the required output
			line = line.replace("\t", "=");
			String key = line.substring(0, line.indexOf("#"));
			String value = line.substring(line.lastIndexOf("#") + 1);			
			context.write(new Text(key), new Text(value));
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context) throws IOException, InterruptedException {

			//get the number of files in the input folder
			Configuration conf = context.getConfiguration();
			Double size = Double.parseDouble(conf.get("name"));

			double count = 0.0;
			ArrayList<String> value = new ArrayList<String>();

			// Store the input value in arraylist and get count for documents containing term
			for(Text t : counts){
				String value1 = t.toString();
				count++;
				value.add(value1);
			}

			// Separate the file name from the value, append it to the key and calculate IDF 
			for(int i=0;i<value.size();i++){
				String fn = value.get(i);
				String file = fn.substring(0, fn.lastIndexOf("="));
				Double value1 = Double.parseDouble(fn.substring(fn.indexOf("=")+1));
				context.write(new Text(word + "#####" + file), new DoubleWritable(calcInverseDocFreq(size,count)*value1));
			}
		}

		// Calculate Inverse Document Frequency
		private double calcInverseDocFreq(double DocTotal, double doccontainterm) {
			return Math.log10(1+(DocTotal / doccontainterm));
		}

	}
}
