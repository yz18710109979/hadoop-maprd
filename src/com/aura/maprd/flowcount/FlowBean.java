package com.aura.maprd.flowcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{

	private Integer upflow;
	private Integer downflow;
	private Integer sumflow;
	public Integer getUpflow() {
		return upflow;
	}
	public void setUpflow(Integer upflow) {
		this.upflow = upflow;
	}
	public Integer getDownflow() {
		return downflow;
	}
	public void setDownflow(Integer downflow) {
		this.downflow = downflow;
	}
	public Integer getsumflow() {
		return sumflow;
	}
	public void setsumflow(Integer sumflow) {
		this.sumflow = sumflow;
	}
	public FlowBean(Integer upflow, Integer downflow) {
		super();
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = this.upflow+this.downflow;
	}
	//无参构造。必须要
	public FlowBean() {
		super();
	}
	
	//必须需要toString方法,写出到hdfs的数据格式
	@Override
	public String toString() {
		return upflow + "\t" + downflow + "\t" + sumflow;
	}
	//序列化方法 hadoop是轻量级的序列化，只针对属性进行序列化
	//参数
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(upflow);
		out.writeInt(downflow);
		out.writeInt(sumflow);
	}
	//反序列方法,反序列化
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upflow = in.readInt();
		this.downflow = in.readInt();
		this.sumflow = in.readInt();
	}
}
