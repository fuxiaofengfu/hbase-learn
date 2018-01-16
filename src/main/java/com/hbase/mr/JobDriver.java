package com.hbase.mr;

import org.apache.hadoop.util.ProgramDriver;

public class JobDriver {

	public static void main(String[] args) {
		ProgramDriver programDriver = new ProgramDriver();
		try {
			//transfer -Dinput.path= -Doutput.path= -Dtable.name=
			programDriver.addClass("transfer", TransferOrcToHbaseJob.class,"将数据文件转换为hbase的HFile");
			//分组取出分组中最大值
			programDriver.addClass("hivegroup", HiveGroupJob.class,"将user_install_status取出最大的uptime出来");
			//将hfile转为orc文件
			programDriver.addClass("hfiletoorc", HFileToOrcJob.class,"将hfile文件转化为orc文件");
			programDriver.run(args);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}
	}
}
