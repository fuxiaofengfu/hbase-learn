package com.hbase.mr;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyWritable extends BinaryComparable
		implements WritableComparable<BinaryComparable> {



	/**
	 * Return n st bytes 0..n-1 from {#getBytes()} are valid.
	 */
	@Override
	public int getLength() {
		return 0;
	}

	/**
	 * Return representative byte array for this instance.
	 */
	@Override
	public byte[] getBytes() {
		return new byte[0];
	}

	/**
	 * Serialize the fields of this object to <code>out</code>.
	 *
	 * @param out <code>DataOuput</code> to serialize this object into.
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {

	}

	/**
	 * Deserialize the fields of this object from <code>in</code>.
	 * <p>
	 * <p>For efficiency, implementations should attempt to re-use storage in the
	 * existing object where possible.</p>
	 *
	 * @param in <code>DataInput</code> to deseriablize this object from.
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

	}
}
