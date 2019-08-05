package com.aura.maprd.groups.g01;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/*
 * 自定义分组
 * 		1）写一个类   继承  WritableComparator
		2）重写  compare 分组核心方法
			course
		3）job指定
 */
public class MyGroup extends WritableComparator{

	//重写 compare
	
	
	/*
	 * 空指针异常  null .方法|属性
	 * 参数a   参数b null
	 * 
	 *   protected WritableComparator() {
		    this(null);
		  }
		  
		  
		 protected WritableComparator(Class<? extends WritableComparable> keyClass) {
		    this(keyClass, null, false);
		  }
		  
		  参数1 mapkeyclass    参数2 配置文件对象  参数3：是否构建mapkeyclass实例
	    protected WritableComparator(Class<? extends WritableComparable> keyClass,
                           Configuration conf,
                           boolean createInstances)
	 */
		
		
	//写当前类的构造方法    调用父类的  3个参数的  将第三个参数  true 初始化mapkeyclass
	public MyGroup() {
		//参数1 mapkeyclass 参数2：是否构建对象
		super(ScoreBean.class,true);
		
	}
	//指定  分组的方法  按 照科目    return a.getCourse  b.getCourse
	//两个参数   代表map key  
	
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		ScoreBean asb=(ScoreBean)a;
		ScoreBean bsb=(ScoreBean)b;
		return asb.getCourse().compareTo(bsb.getCourse());
	}
}
