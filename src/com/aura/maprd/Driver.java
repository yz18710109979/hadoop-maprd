package com.aura.maprd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * ��װmap reduce 
 * 
 * ��  map   reduce ��װһ��job��
 */
public class Driver {
	//args ���� ����̨����   ����1 agrs[0]  ����2  args[1]  �������ʹ�ÿո����
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//��ȡ��Ⱥ�������ļ�
		Configuration conf=new Configuration();
		//����job   ����һ��job����
		Job job=Job.getInstance(conf);
		
		//����job�ķ�װ  
		//ָ��jar�����е� ����
		/*
		 * ��ȡclass 
		 * 1)������class
		 * 2������getClass
		 * 3)Class.forName()
		 */
		job.setJarByClass(Driver.class);
		
		//ָ��map �� reduce��Ӧ����
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//ָ��map�����  k v������
		/*
		 * ��ܶ�ȡ�ļ� -----�� mapper ----��reducer
		 * ���͵��������ڣ�
		 * 	����ʱ��Ч  ����ʱ�Զ�����
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//ָ��reduce���������  ָ�����������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//ָ������·��
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//ָ�����·��
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//�����ӡ������־
		//job.submit();
		//���� ���� �Ƿ��ӡ��־
		job.waitForCompletion(true);
		
	}

}
