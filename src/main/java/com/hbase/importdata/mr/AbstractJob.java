package com.hbase.importdata.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public abstract class AbstractJob extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {

		Job job = getJob(args);
		boolean b = job.waitForCompletion(true);
		return b ? 0 : 1;
	}

	public abstract Job getJob(String[] args);

	public abstract String getJobName();


}
