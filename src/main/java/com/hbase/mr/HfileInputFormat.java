package com.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义输入
 */
public class HfileInputFormat extends FileInputFormat<ImmutableBytesWritable,KeyValue> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public RecordReader<ImmutableBytesWritable, KeyValue> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

		return new HfileRecorder(split,context);
	}

	private class HfileRecorder extends RecordReader<ImmutableBytesWritable, KeyValue>{

		private HFile.Reader reader;
		private HFileScanner hFileScanner;
		private long readnum = 0;

		public HfileRecorder(InputSplit split, TaskAttemptContext context) throws IOException {
			FileSplit fileSplit = (FileSplit)split;
			Configuration configuration = context.getConfiguration();
			this.reader = HFile.createReader(FileSystem.get(configuration), fileSplit.getPath(), new CacheConfig(configuration), configuration);
			reader.loadFileInfo();
			this.hFileScanner = this.reader.getScanner(false,false);;
			this.hFileScanner.seekTo();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context){

		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			readnum ++;
			return this.hFileScanner.next();
		}

		@Override
		public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
			return new ImmutableBytesWritable(this.hFileScanner.getKeyValue().getRowArray());
		}

		@Override
		public KeyValue getCurrentValue() throws IOException, InterruptedException {
			return (KeyValue)this.hFileScanner.getKeyValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return this.readnum / this.reader.getEntries();
		}

		@Override
		public void close() throws IOException {
			this.reader.close();
		}
	}
}
