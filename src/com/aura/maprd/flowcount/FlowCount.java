package com.aura.maprd.flowcount;

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

import com.aura.maprd.Driver;
import com.aura.maprd.WordCountMapper;
import com.aura.maprd.WordCountReducer;

public class FlowCount {

	static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		Text mk=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//获取
			String line = value.toString();
			//	切
			String[] flow_data = line.split("\t");
			//封装
			if(flow_data.length==11){
				mk.set(flow_data[1]);
				FlowBean fb=new FlowBean(
						Integer.parseInt(flow_data[flow_data.length-3]), 
						Integer.parseInt(flow_data[flow_data.length-2]));
				context.write(mk, fb);
			}
		}
		
	}
	static class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, 
				Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			int sum_upflow=0;
			int sum_downflow=0;
			for(FlowBean v:values){
				sum_upflow+=v.getUpflow();
				sum_downflow+=v.getDownflow();
			}
			//封装
			FlowBean fb=new FlowBean(sum_upflow, sum_downflow);
			context.write(key, fb);
			
		}
	}
	public static void main(String[] args) throws Exception {
		//获取集群的配置文件
		Configuration conf=new Configuration();
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
		job.setJarByClass(FlowCount.class);

		//指定map 和 reduce对应的类
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);

		//指定map输出的  k v的类型
		/*
		 * 框架读取文件 -----》 mapper ----》reducer
		 * 泛型的作用周期：
		 * 	编译时生效  运行时自动擦除
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		//指定reduce输出的类型  指定最终输出的
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		//指定输入路径
		FileInputFormat.addInputPath(job, new Path("E:\\hadoop\\doc\\flow"));

		//指定输出路径
		FileSystem fs = FileSystem.get(conf);
		Path out = new Path("E:\\hadoop\\doc\\flow\\flow_out");
		if(fs.exists(out)) {
			fs.delete(out, true);
		}
		FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop\\doc\\flow\\flow_out"));
		//不会打印运行日志
		//job.submit();
		//参数 代表 是否打印日志
		job.waitForCompletion(true);
	}
}
