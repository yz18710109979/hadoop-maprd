package com.aura.maprd;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * ����ͳ��
 * 1���̳� Reducer
 * 2����д  regduce
 * 
 * 
 * ���ͣ�
 * KEYIN, VALUEIN,   ��Ӧ��map�������k  v������
 * ���մ��������
 * KEYOUT,  �����key������   ����  String
 *  VALUEOUT  �����value������  ���մ�Ƶ  int
 * 
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	/**
	 * map�����
	 * 	hello,1   hi,1  hadoop,1
	 * 	hello,1   hi,1   hello,1
	 * mapreduce����� ����map����Ľ��������������   ��������˳��
	 * 		���ջ�����ݽ��з���
	 * 		��map�������key��ͬ�ķֵ�һ��
	 * 		����Ľ���ͬ�ĵ��ʷֵ�һ��
	 * ��������֮��
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
	 * reduce���ܵ����� ʵ�����Ǿ�������֮�������
	 * 
	 * 1)������
	 * key=hello  values==>  <1,1,1>
	 * ����1��ÿһ���һ��key 
	 * ����2: ÿһ���е�  ���е�valueֵ
	 * ����3�������Ķ���  �ϣ�map   �£�������  hdfs
	 * 
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		//ѭ������  values ÿһ��ֵ   �ۼ�   ÿһ��  ���ʳ��ִ���
		int sum=0;
		for(IntWritable v:values){
			//hadoop    ��ֵ  --- java ��ֵ  get����
			sum+=v.get();
		}
		IntWritable rv=new IntWritable(sum);
		context.write(key, rv);
		
	}
	

}
