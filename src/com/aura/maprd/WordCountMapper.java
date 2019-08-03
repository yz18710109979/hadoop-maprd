package com.aura.maprd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 * 分而治之
 * 1）继承 Mapper
 * 2)重写  map
 *
 * 泛型：
 * 输入泛型
 * 在进入到  mapper 之前 框架做了
 * 		mapreduce底层框架给的（流  读取）
 * 		line=br.readLine()
 * KEYIN,   输入的key的类型  每一行的标志  底层字节偏移量  每一行的起始偏移量  long
 * VALUEIN,   输入的value的类型  这里指的就是一行内容  string
 * 输出的泛型  map端的处理结果  输出结果 给  Reducer
 * 	根据需求来的
 * KEYOUT,   输出的key的类型  这里指的是每一个单词  String
 * VALUEOUT  输出的value的类型  这里指的就是词频   int
 *
 *注意：
 *整个mapreduce 中  使用hadoop中的类型 不能使用java中的类型
 *整个mapreduce 中  需要频繁的 数据持久化  网络传输的
 *要求 数据类型  具备 序列化  反序列化能力的
 *
 *java中  序列化  反序列化  重
 *	Stu（zs,19）
 *hadoop中  提供了一个全新的序列化  反序列化 工具   Writable
 *	对于我们常用的一些类型  也已经帮实现了对应的序列化  反序列化的类型
 *	int ---IntWritable
 *	long---LongWritable
 *	double---DoubleWritable
 *	String---Text
 *	null---NullWritable
 *	......
 *序列化反序列化类型：java---Serializable
 *	1)固化（持久化）磁盘
 *	2）网络传输
 *
 *序列化： 对象--- 01010101
 *反序列化：0101---对象
 *
 *
 * 注意：整个mapreduce计算中 数据传输  都是 k-v键值对形式
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * map端  核心逻辑
     *
     * 1）参数
     * 参数1：一行偏移量
     * 参数2：一行内容
     * 参数3：上下文对象  传输  对上：框架  对下：reduce
     *
     * 2）调用频率
     * 一行一次
     * 每次只能处理一行内容的
     *
     *
     * hello	word	hello	ww  key=0   value=hello	word	hello	ww
     lily	hadoop
     hadoop	spark
     hive	spark
     hive	hadoop
     */
    protected void map(LongWritable key, Text value,Context context)
            throws IOException,InterruptedException {
        //实现逻辑
        //获取一行内容 将hadoop类型---java Text--->String
        //br.readLine
        String line = value.toString();
        //切分每一个单词  一行单词
        String[] words = line.split("\t");
        //hello，1	word,1	hello,1	ww,1
        //循环遍历每一个单词  封装k-v   发送给reduce
        for(String w:words){
            //write方法  发送的时候  k-v  参数1：发送的k 单词  参数2：发送的value 1
            //将java--> hadoop
            //String--Text
            Text mk=new Text(w);
            IntWritable mv=new IntWritable(1);
            context.write(mk, mv);
        }

    };
}
