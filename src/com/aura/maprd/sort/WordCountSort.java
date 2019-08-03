package com.aura.maprd.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountSort {

	/*
	 * map端：
		key：-词频  intwritable
		value：单词  text
	 */
	static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		IntWritable mk=new IntWritable();
		Text mv=new Text();
		//hadoop  24
		@Override
		protected void map(LongWritable key, Text value, 
				Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] datas = line.split("\t");
			mk.set(-Integer.parseInt(datas[1]));
			mv.set(datas[0]);
			context.write(mk, mv);
		}
		
	}
	/*
	 * shuffle:
	 * 1)排序
	 * 	-24	hadoop  
		-10	hello   
		-17	hive    
		-7	lily    
		-18	spark   
		-6	word    
		-4	ww  
		
		-24	hadoop 
		-18	spark
		-17	hive
		-10	hello
		-7	lily
		-6	word
		-4	ww
		2）分组  -词频
		-24	hadoop 
		-24 hive
		
		-18	spark
		
		-17	hive
		
		-10	hello
		
		-7	lily
		
		-6	word
		
		-4	ww
		
		
		
		    
	 */
	static class MyReducer extends Reducer<IntWritable, Text, Text, IntWritable>{
		IntWritable rv=new IntWritable();
		//key 相同词频   values：相同词频的所有单词
		//
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			//循环遍历   输出
			for(Text v:values){
				rv.set(-key.get());
				context.write(v, rv);
			}
		}
		
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		//设置代码提交用户
		//System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		//获取集群的配置文件
		Configuration conf=new Configuration();
		//conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		
		//启动job   构建一个job对象
		Job job=Job.getInstance(conf);
		
		//进行job的封装  
		//指定jar包运行的 主类
		/*
		 * 获取class 
		 * 1)类名。class
		 * 2）对象。getClass
		 * 3)Class.forName()
		 */
		job.setJarByClass(WordCountSort.class);
		
		//指定map 和 reduce对应的类
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		//指定map输出的  k v的类型
		/*
		 * 框架读取文件 -----》 mapper ----》reducer
		 * 泛型的作用周期：
		 * 	编译时生效  运行时自动擦除
		 */
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//指定reduce输出的类型  指定最终输出的
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//指定输入路径
		FileInputFormat.addInputPath(job, new Path("E:\\hadoop\\doc\\sort"));
		
		//指定输出路径
		FileSystem fs=FileSystem.get(conf);
		Path out=new Path("E:\\hadoop\\doc\\sort\\output");
		if(fs.exists(out)){
			fs.delete(out, true);
		}
		FileOutputFormat.setOutputPath(job,out );
		//不会打印运行日志
		//job.submit();
		//参数 代表 是否打印日志
		job.waitForCompletion(true);
		
	}
	
}
