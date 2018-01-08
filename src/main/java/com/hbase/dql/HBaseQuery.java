package com.hbase.dql;

import com.hbase.util.HBaseConnectionUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class HBaseQuery {

	public static boolean delete(String tableName, String row, String family, String qualifier) {
		Connection connection = null;
		try {
			connection = HBaseConnectionUtil.getConnection();
			TableName tn = TableName.valueOf(tableName);
			Table t1 = connection.getTable(tn);
			Delete delete = new Delete(Bytes.toBytes(row));
			delete.addColumn(family.getBytes(), qualifier.getBytes());
			t1.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			HBaseConnectionUtil.close(connection);
		}
		return false;
	}

	public static boolean delete(String tableName, String row, String family) {
		Connection connection = null;
		try {
			connection = HBaseConnectionUtil.getConnection();
			TableName tn = TableName.valueOf(tableName);
			Table t1 = connection.getTable(tn);
			Delete delete = new Delete(Bytes.toBytes(row));
			delete.addFamily(Bytes.toBytes(family));
			t1.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			HBaseConnectionUtil.close(connection);
		}
		return false;
	}


	public static void put(String tableName, String row, String family, String qualifier, String value) {
		Connection connection = null;
		try {
			TableName tn = TableName.valueOf(tableName);
			Put put = new Put(Bytes.toBytes(row));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			connection = HBaseConnectionUtil.getConnection();
			Table table = connection.getTable(tn);
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			HBaseConnectionUtil.close(connection);
		}
	}

	public static Result get(String tableName,String row,String family,String qualifier){
		Connection connection = null;
		try {
			TableName tn = TableName.valueOf(tableName);
			Get get = new Get(Bytes.toBytes(row));
			get.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier));

			connection = HBaseConnectionUtil.getConnection();
			Table table = connection.getTable(tn);
			Result result = table.get(get);

			Cell[] cells = result.rawCells();
			for(Cell cell : cells){
				System.out.println(
								Bytes.toString(CellUtil.cloneRow(cell)) +"\t" +
								Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
								Bytes.toString(CellUtil.cloneQualifier(cell))+"\t" +
								Bytes.toString(CellUtil.cloneValue(cell)) +"\t" +
								Bytes.toString(CellUtil.getTagArray(cell))+ "\t" +
								cell.getTimestamp()
				);
			}
			return result;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			HBaseConnectionUtil.close(connection);
		}
		return null;
	}

	public static Result scan(String tableName){
		Connection connection = HBaseConnectionUtil.getConnection();
		Scan scan = new Scan();
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			ResultScanner scanner = table.getScanner(scan);

			Iterator<Result> iterator = scanner.iterator();
			while (iterator.hasNext()){
				Result result = iterator.next();
				System.out.println(result.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		//delete("t1","row1","f3","name");
		put("t1","row1","f3","中文列","中文啦");
		Result  s = get("t1","row1","f3","中文列");
		//System.out.println(s);
		//delete("t1","row1","f3","ff");
	}
}
