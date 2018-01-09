package com.hbase.filter;

import com.hbase.util.HBaseConnectionUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FilterQuery {


	public static void rowValue(String tableName,String family,String qualifier,String value) {

		Connection connection = null;
		try {
			connection = HBaseConnectionUtil.getConnection();
			TableName tableName1 = TableName.valueOf(tableName);
			Table table = connection.getTable(tableName1);
			SingleColumnValueFilter singleColumnValueFilter =
					new SingleColumnValueFilter(Bytes.toBytes(family),
							Bytes.toBytes(qualifier),
							CompareFilter.CompareOp.EQUAL,Bytes.toBytes(value));
			singleColumnValueFilter.setFilterIfMissing(true);
			Scan scan = new Scan();
			scan.setFilter(singleColumnValueFilter);
			iterableResult(table, scan);
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			HBaseConnectionUtil.close(connection);
		}
	}


	public static void qualifier(String tableName,String qualifier){

		Connection connection = null;
		try {
			connection = HBaseConnectionUtil.getConnection();
			TableName tableName1 = TableName.valueOf(tableName);
			Table table = connection.getTable(tableName1);
			QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(qualifier)));
			Scan scan = new Scan();
			scan.setFilter(qualifierFilter);
			iterableResult(table, scan);
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			HBaseConnectionUtil.close(connection);
		}

	}

	/**
	 *
	 * 这个可以用来在结果中过滤不要的列
	 * @param tableName
	 * @param offset  从第几列开始读取  从0开始
	 * @param limit   读取的列的个数
	 */
	public static void pageQuery(String tableName,int offset,int limit){
		Connection connection = null;
		try {
			connection = HBaseConnectionUtil.getConnection();
			TableName tableName1 = TableName.valueOf(tableName);
			Table table = connection.getTable(tableName1);
			ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter(limit,offset);
			Scan scan = new Scan();
			scan.setFilter(columnPaginationFilter);
			iterableResult(table, scan);
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			HBaseConnectionUtil.close(connection);
		}
	}

	/**
	 * 分页查询
	 * @param tableName
	 * @param startRow
	 * @param pageSize
	 */
	public static void pageQuery(String tableName,String startRow,int pageSize){
		Connection connection = null;
		try {
			connection = HBaseConnectionUtil.getConnection();
			TableName tableName1 = TableName.valueOf(tableName);
			Table table = connection.getTable(tableName1);
			PageFilter pageFilter = new PageFilter(pageSize);
			Scan scan = new Scan();
			if(null != startRow && startRow.length() >=1){
				scan.setStartRow(Bytes.toBytes(startRow));
			}
			scan.setFilter(pageFilter);
			iterableResult(table, scan);
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			HBaseConnectionUtil.close(connection);
		}
	}

	private static List<String> iterableResult(Table table, Scan scan) throws IOException {
		Iterator<Result> iterator = table.getScanner(scan).iterator();
		List<String> strings = new ArrayList<>();
		while (iterator.hasNext()){
			Result next = iterator.next();
			Cell[] cells = next.rawCells();
			for(Cell cell : cells){
				String vstr = Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +
						Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
						Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
						Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
						cell.getTimestamp();
				System.out.println(vstr);
				strings.add(vstr);
			}
		}
		return strings;
	}

	public static void main(String[] args) {
		pageQuery("t1","row3",4);
	}

}
