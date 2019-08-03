package com.aura.maprd.local.run;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * 组装map reduce
 *
 * 将  map   reduce 封装一个job中
 */
public class Driver {
	//args 接受 控制台传参   参数1 agrs[0]  参数2  args[1]  多个参数使用空格隔开
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//获取集群的配置文件
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://hdp01:9000");
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
		job.setJarByClass(Driver.class);

		//指定map 和 reduce对应的类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		//指定map输出的  k v的类型
		/*
		 * 框架读取文件 -----》 mapper ----》reducer
		 * 泛型的作用周期：
		 * 	编译时生效  运行时自动擦除
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//指定reduce输出的类型  指定最终输出的
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//指定输入路径
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path("E:\\hadoop\\doc\\wc"));
		
		//指定输出路径
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop\\doc\\wc\\output"));
		//不会打印运行日志
		//job.submit();
		//参数 代表 是否打印日志
		job.waitForCompletion(true);
		
	}

}
