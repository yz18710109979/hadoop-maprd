package com.aura.maprd.partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowPartition {
	/*
	 *	map：
			key：手机号
			value：其他  Text 
		shuffle：
			自定义分区
			按照手机号归属地  分区
			排序  分组
		13480253104	2494800	2494800	4989600
	 */
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text mk = new Text();
		Text mv = new Text();
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) 
				throws InterruptedException, IOException {
			String line = value.toString();
			String[] datas = line.split("\t");
			mk.set(datas[0]);
			//line.substring(line.indexOf(datas[1]));
			mv.set(datas[1]+"\t"+datas[2]+"\t"+datas[3]);
			context.write(mk, mv);
		};
	}
	/*
	 * shuffle:
	 * 	1)分区  map key前3位
	 * 134--136   
	 * 137---138
	 * 139---159
	 * 其他
	 * 
	 * 2）排序  map key
	 * 每一个分区内部  分别排序
	 *	13480253104	2494800	2494800	4989600
		13502468823	101663100	1529437140	1631100240
		13560436666	15467760	13222440	28690200
		13560439658	28191240	81663120	109854360
		13602846565	26860680	40332600	67193280
		13660577991	96465600	9563400	106029000
	3）分组  map key
		相同手机号分在一组
	 */
	/*
	 * 		reduce端：
			每一个分区的数据  排序  分组 
			直接输出
	 */
	//每一个reducetask 都会实例化MyReducer  每一个对象中 反复reduce
	//针对的是每一个分区
	static class MyReducer extends Reducer<Text, Text, Text, NullWritable>{
		Text rk=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, 
				Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//循环遍历输出
			for(Text v:values){
				rk.set(key.toString()+"\t"+v.toString());
				context.write(rk, NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
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
		job.setJarByClass(FlowPartition.class);
		
		//指定map 和 reduce对应的类
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		//指定map输出的  k v的类型
		/*
		 * 框架读取文件 -----》 mapper ----》reducer
		 * 泛型的作用周期：
		 * 	编译时生效  运行时自动擦除
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//指定reduce输出的类型  指定最终输出的
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//指定分区类
		job.setPartitionerClass(MyPartitioner.class);
		
		//指定reducetask的个数  与分区一一对应的
		//Illegal partition for 18211575961 (4)
		job.setNumReduceTasks(5);
		
		//指定输入路径
		FileInputFormat.addInputPath(job, new Path("E:\\hadoop\\doc\\flow\\flow_sort_out"));
		
		//指定输出路径
		FileSystem fs=FileSystem.get(conf);
		Path out=new Path("E:\\hadoop\\doc\\flow\\flow_partition_out");
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
