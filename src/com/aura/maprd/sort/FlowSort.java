package com.aura.maprd.sort;

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

public class FlowSort {

	/*map：
	key: 总流量 降 +  上行 升
		自定义类  （总流量  上行流量  -- 手机号 下行流量）
		1)实现   WritableComparable
		2）重写  compareTo   write   readFields
	value:NullWritable*/
	static class MyMapper extends Mapper<LongWritable, Text, FlowSortBean, NullWritable>{
		FlowSortBean fsb=new FlowSortBean();
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			//获取  13480253104	2494800	2494800	4989600
			String[] datas = value.toString().split("\t");
			//封装
			if(datas.length==4){
				fsb.setPhone_num(datas[0]);
				fsb.setUpflow(Integer.parseInt(datas[1]));
				fsb.setDownflow(Integer.parseInt(datas[2]));
				fsb.setSumflow(Integer.parseInt(datas[3]));
				context.write(fsb, NullWritable.get());
			}
		}
	}

	/*
	 * shuffle:
	 * 	1)排序
	 * 	public int compareTo(FlowSortBean o) {
		
		int tmp=o.getSumflow()-this.getSumflow();
		if(tmp==0){
			tmp=this.getUpflow()-o.getUpflow();
		}
		
		return tmp;
	}
	
	排序  注重  大小值
	
		2）分组  
		按照map key分组的、
		自定义类型  
			默认按照排序的compareTo() 分  是否返回0
		默认认为   compareTo 所有属性相同   一组
		总流量   上行流量相同的为一组
			
	 */
	
		
	/*
	 * 接受的数据：
	 * 	相同总流量  相同的上行流量的所有数据分到一组
	 * key:相同总流量  相同的上行流量  fsb
	 * [fsb1    13480253104	2494800	2494800	4989600,null
	 * fsb2    13480253105	2494800	2494800	4989600,null]
	 * values:
	 * 	null   null
	 * reduce:
	排好序  分好组的数据
	循环遍历输出
	 */
	static class MyReducer extends Reducer<FlowSortBean, NullWritable, FlowSortBean, NullWritable>{
		@Override
		protected void reduce(FlowSortBean key, Iterable<NullWritable> values,
				Context context)
				throws IOException, InterruptedException {
			for(NullWritable v:values){
				context.write(key, v);
			}
			
		}
		
	}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
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
		job.setJarByClass(FlowSort.class);
		
		//指定map 和 reduce对应的类
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		//指定map输出的  k v的类型
		/*
		 * 框架读取文件 -----》 mapper ----》reducer
		 * 泛型的作用周期：
		 * 	编译时生效  运行时自动擦除
		 */
		job.setMapOutputKeyClass(FlowSortBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//指定reduce输出的类型  指定最终输出的
		job.setOutputKeyClass(FlowSortBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		//指定reducetask的个数
//		job.setNumReduceTasks(1);
		
		
		
		//指定输入路径
		FileInputFormat.addInputPath(job, new Path("E:\\hadoop\\doc\\\\flow\\flow_out"));
		
		//指定输出路径
		FileSystem fs=FileSystem.get(conf);
		Path out=new Path("E:\\hadoop\\doc\\flow\\flow_sort_out");
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
