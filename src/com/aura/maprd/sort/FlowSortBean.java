package com.aura.maprd.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*
 * map key 
 * 排序  序列化  反序列化
 * 1）实现  WritableComparable
 * 2)重写 write readFields compareTo
 * 
 * 泛型 用于比较的类型
 */
public class FlowSortBean implements WritableComparable<FlowSortBean>{
	private String phone_num;
	private int upflow;
	private int downflow;
	private int sumflow;
	public String getPhone_num() {
		return phone_num;
	}
	public void setPhone_num(String phone_num) {
		this.phone_num = phone_num;
	}
	public int getUpflow() {
		return upflow;
	}
	public void setUpflow(int upflow) {
		this.upflow = upflow;
	}
	public int getDownflow() {
		return downflow;
	}
	public void setDownflow(int downflow) {
		this.downflow = downflow;
	}
	public int getSumflow() {
		return sumflow;
	}
	public void setSumflow(int sumflow) {
		this.sumflow = sumflow;
	}
	public FlowSortBean() {
		super();
	}
	public FlowSortBean(String phone_num, int upflow, int downflow, int sumflow) {
		super();
		this.phone_num = phone_num;
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = sumflow;
	}
	@Override
	public String toString() {
		/*
		 * \001
		 */
		return phone_num + "\t" + upflow + "\t" + downflow + "\t"
				+ sumflow;
	}
	//序列化
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone_num);
		out.writeInt(upflow);
		out.writeInt(downflow);
		out.writeInt(sumflow);
		
	}
	//反序列化
	@Override
	public void readFields(DataInput in) throws IOException {
		this.phone_num=in.readUTF();
		this.upflow=in.readInt();
		this.downflow=in.readInt();
		this.sumflow=in.readInt();
		
	}
	
	//核心  比较方法
	/*
	 * 先按照总流量   降
	 * 	在按照上行  升
	 */
	//参数  用于比较的其他对象   this 当前对象
	/*
	 * this-o 升
	 * o-this  降
	 */
	@Override
	public int compareTo(FlowSortBean o) {
		/*
		 * +  -   0
		 */
		int tmp=o.getSumflow()-this.getSumflow();
		if(tmp==0){
			tmp=this.getUpflow()-o.getUpflow();
		}
		
		return tmp;
	}
}
