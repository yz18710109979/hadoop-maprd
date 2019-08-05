package com.aura.maprd.groups.g01;

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


public class GroupTopN {

	/*
	 * map端：
		key：平均分  科目
			自定义类
			course avgscore  name
		value：null
	 */
	static class MyMapper extends  Mapper<LongWritable, Text, ScoreBean, NullWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			//computer,huangxiaoming,85,86,41,75,93,42,85
			String[] datas = value.toString().split(",");
			int sum=0;
			for(int i=2;i<datas.length;i++){
				sum+=Integer.parseInt(datas[i]);
			}
			double avg=(double)sum/(datas.length-2);
			ScoreBean sb=new ScoreBean(datas[0], datas[1], avg);
			context.write(sb, NullWritable.get());
		}
	}
	
	
	//shuffle 
	/*
	 * 排序  按照mapley 排序  按照平均分 降序
	 * 
	 * 分组：
	 */
	static class MyReducer extends Reducer<ScoreBean, NullWritable, ScoreBean, NullWritable>{
		@Override
		protected void reduce(ScoreBean key, Iterable<NullWritable> values,
				Context context)
				throws IOException, InterruptedException {
			//输出组标
			System.out.println("=========================="+key.toString());
			//输出组表示
			int count=0;
			for(NullWritable v:values){
				count++;
				//3次
				System.out.println("@@@@@@@@@@"+key.toString());
				context.write(key, v);
				if(count==3){
					break;
				}
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
		job.setJarByClass(GroupTopN.class);
		
		//指定map 和 reduce对应的类
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		//指定map输出的  k v的类型
		/*
		 * 框架读取文件 -----》 mapper ----》reducer
		 * 泛型的作用周期：
		 * 	编译时生效  运行时自动擦除
		 */
		job.setMapOutputKeyClass(ScoreBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//指定reduce输出的类型  指定最终输出的
		job.setOutputKeyClass(ScoreBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		//指定分组类
		job.setGroupingComparatorClass(MyGroup.class);
		
		
		
		//指定输入路径
		FileInputFormat.addInputPath(job, new Path("E:\\hadoop\\doc\\groups"));
		
		//指定输出路径
		FileSystem fs=FileSystem.get(conf);
		Path out=new Path("E:\\hadoop\\doc\\groups_out");
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
