package com.hbase.mr;

import com.hbase.ddl.HBaseDDL;
import com.hbase.util.HBaseConnectionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;

public class TransferOrcToHbaseJob extends AbstractJob {

	private Configuration configuration = null;

	public Configuration getConfiguration() {
		return configuration;
	}
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public Job getJob(String[] args) {

		Job job = null;
		Connection connection = null;
		Admin admin = null;
		try {
			Configuration configuration = HBaseConfiguration.create(getConf());
			configuration.set(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST,"true");
			configuration.set("mapreduce.task.classpath.user.precedence","true");
			super.setConf(configuration);
			setConfiguration(configuration);
			//get the args w/o generic hadoop args
			connection = ConnectionFactory.createConnection(configuration);
			admin = connection.getAdmin();
			TableName tableName = TableName.valueOf(configuration.get(TABLE_NAME));
			if (!admin.tableExists(tableName)) {
				//throw new RuntimeException(MessageFormat.format("表{0}不存在", configuration.get(TABLE_NAME)));
				HBaseDDL.createTable(tableName.getNameAsString(),"f1");
			}
			HTable table = (HTable) connection.getTable(tableName);
			job = Job.getInstance(configuration, getJobName());

			job.setJarByClass(TransferOrcToHbaseJob.class);
			job.setMapperClass(TransferMapper.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);
			job.setInputFormatClass(OrcInputFormat.class);

			FileInputFormat.setInputPaths(job, configuration.get(INPUT_PATH));
			String outputPath = configuration.get(OUTPUT_PATH);
			Path outPath = new Path(outputPath);
			FileSystem fileSystem = FileSystem.get(configuration);
			if (fileSystem.exists(outPath)) {
				fileSystem.delete(outPath, true);
			}
			FileOutputFormat.setOutputPath(job, outPath);
			HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), table.getRegionLocator());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			HBaseConnectionUtil.close(connection, admin);
		}
		//TableMapReduceUtil.addDependencyJars(null);
		//TableMapReduceUtil.initTableReducerJob();
		return job;
	}

	@Override
	public String getJobName() {
		return "orctransferhbase";
	}


	private static class TransferMapper extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put> {

		ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
		byte[] family = Bytes.toBytes("f1");
		byte[] pkgname = Bytes.toBytes("pkgname");
		byte[] uptime = Bytes.toBytes("uptime");
		byte[] type = Bytes.toBytes("type");
		byte[] country = Bytes.toBytes("country");
		byte[] gpcategory = Bytes.toBytes("gpcategory");

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {

			String aid = value.getFieldValue(0).toString();

			if (StringUtils.isEmpty(aid)) return;
			//value.getSchema().getId()
			Put put = new Put(Bytes.toBytes(aid));
			WritableComparable pkgnamev = value.getFieldValue(1);
			WritableComparable uptimev = value.getFieldValue(2);
			WritableComparable typev = value.getFieldValue(3);
			WritableComparable countryv = value.getFieldValue(4);
			WritableComparable gpcategoryv = value.getFieldValue(5);
			if (null != pkgnamev) {
				put.addColumn(family, pkgname,Bytes.toBytes(pkgnamev.toString()));
			}
			if (null != uptimev) {
				put.addColumn(family, uptime,Bytes.toBytes(uptimev.toString()));
			}
			if (null != typev) {
				put.addColumn(family, type,Bytes.toBytes(typev.toString()));
			}
			if (null != countryv) {
				put.addColumn(family, country,Bytes.toBytes(countryv.toString()));
			}
			if (null != gpcategoryv) {
				put.addColumn(family, gpcategory,Bytes.toBytes(gpcategoryv.toString()));
			}
			immutableBytesWritable.set(put.getRow());
			context.write(immutableBytesWritable, put);
		}
	}

	public static void main(String[] args) throws Throwable {
		try {
			TransferOrcToHbaseJob transferOrcToHbaseJob = new TransferOrcToHbaseJob();
			int run = ToolRunner.run(transferOrcToHbaseJob, args);
			if( 0 == run){
				Configuration configuration = transferOrcToHbaseJob.getConfiguration();
				// 预分region
				RegionSplitter.main(new String[]{configuration.get(TABLE_NAME),"com.hbase.importdata.SplitRegions","-c","2","-f","f1"});
				// load数据 import org.apache.hadoop.hbase.mapreduce.Driver.main();
				LoadIncrementalHFiles.main(new String[]{configuration.get(OUTPUT_PATH),configuration.get(TABLE_NAME)});
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
