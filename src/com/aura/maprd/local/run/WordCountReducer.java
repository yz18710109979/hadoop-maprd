package com.aura.maprd.local.run;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * 汇总统计
 * 1）继承 Reducer
 * 2）重写  regduce
 *
 *
 * 泛型：
 * KEYIN, VALUEIN,   对应的map的输出的k  v的类型
 * 最终处理结果输出
 * KEYOUT,  输出的key的类型   单词  String
 *  VALUEOUT  输出的value的类型  最终词频  int
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	/**
	 * map输出：
	 * 	hello,1   hi,1  hadoop,1
	 * 	hello,1   hi,1   hello,1
	 * mapreduce框架中 对于map输出的结果会做数据整理   调整数据顺序
	 * 		最终会对数据进行分组
	 * 		将map端输出的key相同的分到一组
	 * 		这里的将相同的单词分到一组
	 * 经过整理之后：
	 * 	group1:
	 * 		hello,1
	 * 		hello,1
	 * 		hello,1
	 * group2:
	 * 		hi,1
	 * 		hi,1
	 * group3:
	 * 		hadoop,1
	 *
	 * reduce接受的数据 实际上是经过分组之后的数据
	 *
	 * 1)参数：
	 * key=hello  values==>  <1,1,1>
	 * 参数1：每一组的一个key
	 * 参数2: 每一组中的  所有的value值
	 * 参数3：上下文对象  上：map   下：输出结果  hdfs
	 *
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		//循环遍历  values 每一个值   累加   每一组  单词出现次数
		int sum=0;
		for(IntWritable v:values){
			//hadoop    数值  --- java 数值  get（）
			sum+=v.get();
		}
		IntWritable rv=new IntWritable(sum);
		context.write(key, rv);
		
	}
	

}
