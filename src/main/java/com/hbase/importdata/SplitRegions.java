package com.hbase.importdata;

import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

public class SplitRegions implements RegionSplitter.SplitAlgorithm {
	/**
	 * Split a pre-existing region into 2 regions.
	 *
	 * @param start first row (inclusive)
	 * @param end   last row (exclusive)
	 * @return the split row to use
	 */
	@Override
	public byte[] split(byte[] start, byte[] end) {
		return new byte[0];
	}

	/**
	 * Split an entire table.
	 *
	 * @param numRegions number of regions to split the table into
	 * @return array of split keys for the initial regions of the table. The
	 * length of the returned array should be numRegions-1.
	 * @throws RuntimeException user input is validated at this time. may throw a runtime
	 *                          exception in response to a parse failure
	 */
	@Override
	public byte[][] split(int numRegions) {

		//GenericUDF


		return new byte[0][];
	}

	/**
	 * In HBase, the first row is represented by an empty byte array. This might
	 * cause problems with your split algorithm or row printing. All your APIs
	 * will be passed firstRow() instead of empty array.
	 *
	 * @return your representation of your first row
	 */
	@Override
	public byte[] firstRow() {
		return new byte[0];
	}

	/**
	 * In HBase, the last row is represented by an empty byte array. This might
	 * cause problems with your split algorithm or row printing. All your APIs
	 * will be passed firstRow() instead of empty array.
	 *
	 * @return your representation of your last row
	 */
	@Override
	public byte[] lastRow() {
		return new byte[0];
	}

	/**
	 * In HBase, the last row is represented by an empty byte array. Set this
	 * value to help the split code understand how to evenly divide the first
	 * region.
	 *
	 * @param userInput raw user input (may throw RuntimeException on parse failure)
	 */
	@Override
	public void setFirstRow(String userInput) {

	}

	/**
	 * In HBase, the last row is represented by an empty byte array. Set this
	 * value to help the split code understand how to evenly divide the last
	 * region. Note that this last row is inclusive for all rows sharing the
	 * same prefix.
	 *
	 * @param userInput raw user input (may throw RuntimeException on parse failure)
	 */
	@Override
	public void setLastRow(String userInput) {

	}

	/**
	 * @param input user or file input for row
	 * @return byte array representation of this row for HBase
	 */
	@Override
	public byte[] strToRow(String input) {
		return new byte[0];
	}

	/**
	 * @param row byte array representing a row in HBase
	 * @return String to use for debug &amp; file printing
	 */
	@Override
	public String rowToStr(byte[] row) {
		return null;
	}

	/**
	 * @return the separator character to use when storing / printing the row
	 */
	@Override
	public String separator() {
		return null;
	}

	/**
	 * Set the first row
	 *
	 * @param userInput byte array of the row key.
	 */
	@Override
	public void setFirstRow(byte[] userInput) {

	}

	/**
	 * Set the last row
	 *
	 * @param userInput byte array of the row key.
	 */
	@Override
	public void setLastRow(byte[] userInput) {

	}
}
