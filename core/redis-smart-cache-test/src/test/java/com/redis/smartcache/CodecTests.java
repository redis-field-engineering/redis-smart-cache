package com.redis.smartcache;

import java.sql.SQLException;

import javax.sql.RowSet;
import javax.sql.RowSetMetaData;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.redis.smartcache.core.codec.RowSetCodec;
import com.redis.smartcache.core.codec.SerializedResultSetCodec;
import com.redis.smartcache.test.RowSetBuilder;

@TestInstance(Lifecycle.PER_CLASS)
class CodecTests {

	private static final int BYTE_BUFFER_CAPACITY = 100;

	private RowSetBuilder rowSetBuilder;

	@BeforeAll
	public void setup() throws SQLException {
		rowSetBuilder = new RowSetBuilder();
	}

	@Test
	void resultSetCodec() throws SQLException {
		RowSetMetaData metaData = rowSetBuilder.metaData(RowSetBuilder.SUPPORTED_TYPES);
		RowSet rowSet = rowSetBuilder.build(metaData, 1000);
		RowSetCodec codec = RowSetCodec.builder().maxByteBufferCapacityMB(BYTE_BUFFER_CAPACITY).build();
		RowSet actual = codec.decodeValue(codec.encodeValue(rowSet));
		rowSet.beforeFirst();
		TestUtils.assertEquals(rowSet, actual);
	}

	@Test
	void serializedResultSetCodec() throws SQLException {
		RowSet rowSet = rowSetBuilder.build(rowSetBuilder.metaData(RowSetBuilder.SUPPORTED_TYPES), 100);
		SerializedResultSetCodec codec = new SerializedResultSetCodec(rowSetBuilder.getRowSetFactory(),
				BYTE_BUFFER_CAPACITY * 1024 * 1024);
		RowSet actual = codec.decodeValue(codec.encodeValue(rowSet));
		rowSet.beforeFirst();
		TestUtils.assertEquals(rowSet, actual);
	}

	public String toString(RowSet rowSet) throws SQLException {
		StringBuilder builder = new StringBuilder();
		int rowIndex = 1;
		while (rowSet.next()) {
			builder.append("\n");
			builder.append(String.format("%2d", rowIndex)).append(":");
			for (int columnIndex = 1; columnIndex <= rowSet.getMetaData().getColumnCount(); columnIndex++) {
				builder.append(" ").append(rowSet.getMetaData().getColumnName(columnIndex)).append("=")
						.append(rowSet.getObject(columnIndex));
			}
			rowIndex++;
		}
		return builder.toString();
	}

}
