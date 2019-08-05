package com.aura.maprd.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区
 * 分区类
	 * 按照手机号分区
	 * 		134---136   bj  part-r-00000   0
			137---138   sz  part-r-00001   1
			139---159   wh  part-r-00002    2
			其他    未名	part-r-00003    3
			4reducetasks -- 4个分区
		1）继承  partitioner
		2)getPartitioner
		
		泛型：map输出的k  v
 *
 */
public class MyPartitioner extends Partitioner<Text, Text>{
	/*默认
	 * numPartitions=== reducetask的个数
	 * 返回值：
	 * 分区编号  0开始  顺序递增
	 */
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		String prefix = key.toString().substring(0, 3);
		int _prefix=Integer.parseInt(prefix);
		/*if(prefix.compareTo("134")>=0 && prefix.compareTo("136")<=0){
			return 0;
		}*/
		if(_prefix>=134 && _prefix<=136){
			return 0;
		}else if(_prefix>=137 && _prefix<=138){
			return 1;
		}else if(_prefix>=139 && _prefix<=159){
			return 2;
		}else{
			return 3;
		}
	}
}
