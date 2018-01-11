package com.hbase.importdata;

import com.hbase.importdata.mr.TransferJob;
import org.apache.hadoop.util.ProgramDriver;

public class JobDriver {

	public static void main(String[] args) {
		ProgramDriver programDriver = new ProgramDriver();
		try {
			programDriver.addClass("transfer", TransferJob.class,"将数据文件转换为hbase的HFile");
			programDriver.run(args);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}
	}
}
