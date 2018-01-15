package com.hbase.mr;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

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

	/**
	 * 处理classpath路径
	 * @param config
	 */
	protected void handleClasspath(Configuration config){
		String[] classpathArray = config.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
		String cp = "$PWD/*:" +  StringUtils.join(classpathArray, ":");
		config.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, cp);
	}
}
