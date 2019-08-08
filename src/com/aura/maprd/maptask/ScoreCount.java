package com.aura.maprd.maptask;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author yz
 * @date 2019/8/3 11:23
 */
public class ScoreCount {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		protected void setup(Context context) throws java.io.IOException ,InterruptedException {
			//
		};
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			super.map(key, value, context);
		}
		protected void cleanup(Context context) throws IOException ,InterruptedException {
			
		};
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{}
	
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf);
    	job.setJarByClass(ScoreCount.class);
    	
    	job.setMapperClass(MyMapper.class);
    	job.setReducerClass(MyReducer.class);
    	
    	//
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(LongWritable.class);
    	//
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(LongWritable.class);
    	
    	FileInputFormat.setMaxInputSplitSize(job, 1);
    	
    	job.setNumReduceTasks(3);
    	
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	job.waitForCompletion(true);
    }
}
