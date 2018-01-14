package com.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.orc.OrcConf;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class HiveGroupJob extends  AbstractJob{

	private final static String ORC_SCHEMA = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";

	@Override
	public Job getJob(String[] args) {
		Job job = null;
		try {
			Configuration conf = getConf();
			new GenericOptionsParser(conf, args);
			OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf,ORC_SCHEMA);
			job = Job.getInstance(getConf(),getJobName());
			job.setJarByClass(HiveGroupJob.class);
			job.setMapperClass(HiveGroupMapper.class);
			job.setCombinerClass(HiveGroupCombiner.class);
			job.setReducerClass(HiveGroupReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(OrcStruct.class);
			job.setInputFormatClass(OrcInputFormat.class);
			job.setOutputFormatClass(OrcOutputFormat.class);
			FileInputFormat.setInputPaths(job,conf.get(INPUT_PATH));
			FileSystem fileSystem = FileSystem.get(conf);
			Path path = new Path(conf.get(OUTPUT_PATH));
			if(fileSystem.exists(path)){
				fileSystem.delete(path,true);
			}
			FileOutputFormat.setOutputPath(job,path);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return job;
	}

	@Override
	public String getJobName() {
		return "hivewindow";
	}

	private static class HiveGroupMapper extends Mapper<NullWritable, OrcStruct,Text, OrcStruct>{

		private Text aid = new Text();

		@Override
		protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {

			WritableComparable fieldValue = value.getFieldValue(0);
			if(null != fieldValue){
				aid.set(fieldValue.toString());
				context.write(aid,value);
			}
		}
	}

	private static class HiveGroupCombiner extends Reducer<Text, OrcStruct,Text, OrcStruct>{

		Text aid = new Text();
		@Override
		protected void reduce(Text key, Iterable<OrcStruct> values, Context context) throws IOException, InterruptedException {
			OrcStruct struct = getMaxOrcstruct(aid, values);
			if(null == struct){
				return;
			}
			context.write(aid,struct);
		}
	}

	private static OrcStruct getMaxOrcstruct(Text aid,Iterable<OrcStruct> values) throws IOException, InterruptedException {
		Iterator<OrcStruct> iterator = values.iterator();
		OrcStruct maxStruct = values.iterator().next();
		WritableComparable fieldValue1 = maxStruct.getFieldValue(2);
		if(null == fieldValue1){
			return null;
		}
		String uptimeStr = fieldValue1.toString();
		long maxuptime = Long.parseLong(uptimeStr);
		while (iterator.hasNext()){
			OrcStruct next = iterator.next();
			WritableComparable fieldValue = next.getFieldValue(2);
			if(null != fieldValue){
				long _uptime = Long.parseLong(fieldValue.toString());
				if(maxuptime < _uptime){
					maxuptime = _uptime;
					maxStruct = next;
				}
			}
		}
		aid.set(maxStruct.getFieldValue(0).toString());
		return maxStruct;
	}

	private static class HiveGroupReduce extends Reducer<Text, OrcStruct,NullWritable, OrcStruct>{
		Text aid = new Text();

		@Override
		protected void reduce(Text key, Iterable<OrcStruct> values, Context context) throws IOException, InterruptedException {
			OrcStruct struct = getMaxOrcstruct(aid, values);
			if(null == struct){
				return;
			}
			context.write(NullWritable.get(),struct);
		}
	}
}
