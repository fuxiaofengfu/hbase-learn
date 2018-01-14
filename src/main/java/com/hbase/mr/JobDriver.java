package com.hbase.mr;

import org.apache.hadoop.util.ProgramDriver;

public class JobDriver {

	public static void main(String[] args) {
		ProgramDriver programDriver = new ProgramDriver();
		try {
			programDriver.addClass("transfer", TransferOrcToHbaseJob.class,"将数据文件转换为hbase的HFile");
			programDriver.addClass("hivegroup", HiveGroupJob.class,"将user_install_status取出最大的uptime出来");
			programDriver.run(args);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}
	}
}
