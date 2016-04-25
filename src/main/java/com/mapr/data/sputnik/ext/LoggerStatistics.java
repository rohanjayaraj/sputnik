package com.mapr.data.sputnik.ext;

import java.text.DecimalFormat;

public class LoggerStatistics {
	private long numMsgs = 0;
	private double totalInsertTime = 0;
	private long totalsize = 0;
	
	public long getNumMsgs(){
		return numMsgs;
	}
	
	public double getTotalInsertTime(){
		return totalInsertTime;
	}
	
	public double getTotalInsertTimeInSec(){
		return (totalInsertTime/1000);
	}
	
	public void incrMsgCount(){
		numMsgs++;
	}
	
	public void addInsertTime(long start, long end){
		double difference = (end - start)/1000000;
		totalInsertTime+=difference;
	}
	
	public void addMsgSize(long size){
		totalsize+=size;
	}
	
	public String getAverageInsertTime(){
		double ave = totalInsertTime/numMsgs;
		DecimalFormat df = new DecimalFormat("#.000000"); 
		return df.format(ave);
	}
	
	public long getNumMsgPerSec(){
		return (long)(numMsgs/(totalInsertTime/1000));
	}
	
	public long getTotalSize(){
		return totalsize;
	}
}
