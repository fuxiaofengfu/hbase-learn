package com.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;

public class HFileToOrcJob extends AbstractJob{

	private final static String ORC_SCHEMA = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";

	@Override
	public Job getJob(String[] args) throws IOException {
		Configuration conf = getConf();

		conf.set("orc.compress", CompressionKind.SNAPPY.name());
		conf.set("orc.create.index", "true");
		conf.set(OrcConf.MAPRED_OUTPUT_SCHEMA.name(),ORC_SCHEMA);
		Job job = Job.getInstance(conf, getJobName());

		job.setJarByClass(HFileToOrcJob.class);
		job.setMapperClass(HFileToOrcMapper.class);
		job.setReducerClass(HFileToOrcReduce.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(OrcStruct.class);

		job.setInputFormatClass(HfileInputFormat.class);
		job.setOutputFormatClass(OrcOutputFormat.class);

		FileInputFormat.setInputPaths(job,conf.get(INPUT_PATH));
		Path out = new Path(conf.get(OUTPUT_PATH));
		FileOutputFormat.setOutputPath(job,out);

		FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(out)){
			fileSystem.delete(out,true);
		}
		return job;
	}

	@Override
	public String getJobName() {
		return "hfiletoorcjob";
	}

	private static class HFileToOrcMapper extends Mapper<ImmutableBytesWritable,KeyValue,ImmutableBytesWritable,Text> {

		Text text = new Text();
		@Override
		protected void map(ImmutableBytesWritable key, KeyValue value, Context context) throws IOException, InterruptedException {

			String aid = Bytes.toString(CellUtil.cloneRow(value));
			String family = Bytes.toString(CellUtil.cloneFamily(value));
			String qualifier = Bytes.toString(CellUtil.cloneQualifier(value));
			String v = Bytes.toString(CellUtil.cloneValue(value));
			long timestamp = value.getTimestamp();
			boolean delete = CellUtil.isDelete(value);
			if(delete){
				return;
			}
			text.set(qualifier);
			context.write(key,text);
		}
	}

	private static class HFileToOrcMapperCombiner extends Reducer<ImmutableBytesWritable,Text,ImmutableBytesWritable,Text>{



		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		}
	}


	private static class HFileToOrcReduce extends Reducer<ImmutableBytesWritable,Text,NullWritable,OrcStruct>{

		TypeDescription typeDescription = TypeDescription.fromString(ORC_SCHEMA);
		OrcStruct value = new OrcStruct(typeDescription);

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {



			context.write(NullWritable.get(),value);

		}
	}

	public static void main(String[] args) {
		try {
			System.exit(ToolRunner.run(new HFileToOrcJob(),args));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
