package com.aura.maprd.groups.g01;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * 自定类    排序
 * @author Yangz
 *
 */
public class ScoreBean implements WritableComparable<ScoreBean>{
	private String course;
	private String name;
	private double avg;
	public String getCourse() {
		return course;
	}
	public void setCourse(String course) {
		this.course = course;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public double getAvg() {
		return avg;
	}
	public void setAvg(double avg) {
		this.avg = avg;
	}
	public ScoreBean(String course, String name, double avg) {
		super();
		this.course = course;
		this.name = name;
		this.avg = avg;
	}
	public ScoreBean() {
		super();
	}
	@Override
	public String toString() {
		return course + "\t" + name + "\t" + avg ;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(course);
		out.writeUTF(name);
		out.writeDouble(avg);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.course=in.readUTF();
		this.name=in.readUTF();
		this.avg=in.readDouble();
		
	}
	/*
	 * 按照平均分  降序
	 */
	@Override
	public int compareTo(ScoreBean o) {
		// TODO Auto-generated method stub
		//93.5     93
		//先按照科目排序
		
		int tmp1=this.getCourse().compareTo(o.getCourse());
		if(tmp1==0){
			double tmp=o.getAvg()-this.getAvg();
			if(tmp>0){
				return 1;
			}else if(tmp<0){
				return -1;
			}else{
				return 0;
			}
		}
		return tmp1;
		
		
	}
}
