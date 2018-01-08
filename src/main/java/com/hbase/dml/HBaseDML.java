package com.hbase.dml;

import com.hbase.util.HBaseConnectionUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDML {

	public static boolean addColumn(String tableName,String familyName){
		Connection connection = null;
		Admin admin = null;
		try {
			connection = HBaseConnectionUtil.getConnection();
			admin = connection.getAdmin();
			TableName tn = TableName.valueOf(tableName);
			if (!admin.tableExists(tn)) return false;
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyName);
			admin.addColumn(tn,hColumnDescriptor);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			HBaseConnectionUtil.close(connection,admin);
		}
		return false;
	}


	/**
	 * 删除列
	 * @return
	 */
	public static boolean deleteColumn(String tableName,String family){

		Connection connection = null;
		Admin admin = null;
		try {
			connection = HBaseConnectionUtil.getConnection();
			admin = connection.getAdmin();
			TableName tn = TableName.valueOf(tableName);
			if (!admin.tableExists(tn)) return true;
			admin.deleteColumn(tn, Bytes.toBytes(family));
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			HBaseConnectionUtil.close(connection,admin);
		}
		return false;
	}

}
