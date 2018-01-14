package com.hbase.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public abstract class AbstractJob extends Configured implements Tool{

	protected static final String TABLE_NAME = "table.name";
	protected static final String INPUT_PATH = "input.path";
	protected static final String OUTPUT_PATH = "output.path";

	@Override
	public int run(String[] args) throws Exception {

		Job job = getJob(args);
		boolean b = job.waitForCompletion(true);
		return b ? 0 : 1;
	}

	public abstract Job getJob(String[] args);

	public abstract String getJobName();


}
