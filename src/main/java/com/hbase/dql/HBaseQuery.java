package com.hbase.dql;

import com.hbase.util.HBaseConnectionUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
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

	public static Result get(String tableName, String row, String family, String qualifier) {
		Connection connection = null;
		try {
			TableName tn = TableName.valueOf(tableName);
			Get get = new Get(Bytes.toBytes(row));
			get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

			connection = HBaseConnectionUtil.getConnection();
			Table table = connection.getTable(tn);
			Result result = table.get(get);

			Cell[] cells = result.rawCells();
			for (Cell cell : cells) {
				System.out.println(
						Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +
								Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
								Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
								Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
								Bytes.toString(CellUtil.getTagArray(cell)) + "\t" +
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

	public static Result scan(String tableName) {
		Connection connection = null;
		try {
			Scan scan = new Scan();
			connection = HBaseConnectionUtil.getConnection();
			Table table = connection.getTable(TableName.valueOf(tableName));
			ResultScanner scanner = table.getScanner(scan);

			Iterator<Result> iterator = scanner.iterator();
			while (iterator.hasNext()) {
				Result result = iterator.next();
				System.out.println(result.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			HBaseConnectionUtil.close(connection);
		}
		return null;
	}

	public static Result scan(String tableName,int pageSize) {
		Connection connection = null;
		try {
			Scan scan = new Scan();
			PageFilter pageFilter = new PageFilter(pageSize);


			scan.setFilter(pageFilter);
			byte[] startRow = null;
			if(null != startRow) scan.setStartRow(startRow);
			connection = HBaseConnectionUtil.getConnection();
			Table table = connection.getTable(TableName.valueOf(tableName));
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> iterator = scanner.iterator();
			int firstRow = 0;
			while (iterator.hasNext()) {
				if(0 == firstRow)continue;
				firstRow ++;
				Result result = iterator.next();
				System.out.println(result.toString());
				startRow = result.getRow();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			HBaseConnectionUtil.close(connection);
		}
		return null;
	}

	public static void main(String[] args) {
		//delete("t1","row1","f3","name");
		//put("t1", "row2", "f3", "中文列", "中文啦");
//		put("t1", "row3", "f3", "中文列", "中文啦");
//		put("t1", "row4", "f3", "中文列", "中文啦");
//		put("t1", "row5", "f3", "中文列", "中文啦");
		//Result s = get("t1", "row1", "f3", "中文列");
		//System.out.println(s);
		//delete("t1","row1","f3","ff");




	}
}
