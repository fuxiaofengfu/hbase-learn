package com.hbase.ddl;

import com.hbase.util.HBaseConnectionUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.text.MessageFormat;

public class HBaseDDL {

	/**
	 * 创建表
	 * @param tableName
	 * @param columnFamilies
	 * @return
	 * @throws Exception
	 */
	public static boolean createTable(String tableName,String... columnFamilies) throws Exception {

		if(null == columnFamilies || columnFamilies.length <=0){
			String message = "表{0}的columnFamily为空";
			throw new Exception(MessageFormat.format(message,tableName));
		}

		Connection connection = null;
		Admin admin = null;
		try {
			connection = HBaseConnectionUtil.getConnection();
			admin = connection.getAdmin();
			TableName tn = TableName.valueOf(tableName);
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tn);
			for (String f:columnFamilies) {
				hTableDescriptor.addFamily(new HColumnDescriptor(f));
			}
			admin.createTable(hTableDescriptor);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			HBaseConnectionUtil.close(admin,connection);
		}
		return false;
	}

	public static boolean dropTable(String tableName){

		Connection connection = null;
		Admin admin = null;
		try {
			connection = HBaseConnectionUtil.getConnection();
			admin = connection.getAdmin();
			TableName tn = TableName.valueOf(tableName);
			// 表不存在直接返回true
			if (!admin.tableExists(tn)) return true;
			admin.disableTable(tn);
			admin.deleteTable(tn);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			HBaseConnectionUtil.close(connection,admin);
		}
		return false;
	}

}
