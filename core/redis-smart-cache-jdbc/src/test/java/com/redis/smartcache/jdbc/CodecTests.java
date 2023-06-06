package com.redis.smartcache.jdbc;

import java.sql.SQLException;

import javax.sql.RowSet;

import org.junit.jupiter.api.Test;

import com.redis.smartcache.jdbc.codec.SerializedResultSetCodec;
import com.redis.smartcache.test.RowSetBuilder;

class CodecTests {

	private static final int BYTE_BUFFER_CAPACITY = 100;

	@Test
	void resultSetCodec() throws SQLException {
		RowSet rowSet = rowSetBuilder().build();
		RowSetCodec codec = new RowSetCodec(BYTE_BUFFER_CAPACITY * 1024 * 1024);
		RowSet actual = codec.decodeValue(codec.encodeValue(rowSet));
		rowSet.beforeFirst();
		Utils.assertEquals(rowSet, actual);
	}

	private RowSetBuilder rowSetBuilder() {
		return new RowSetBuilder(new RowSetFactoryImpl());
	}

	@Test
	void serializedResultSetCodec() throws SQLException {
		RowSet rowSet = rowSetBuilder().build();
		SerializedResultSetCodec codec = new SerializedResultSetCodec(BYTE_BUFFER_CAPACITY * 1024 * 1024);
		RowSet actual = codec.decodeValue(codec.encodeValue(rowSet));
		rowSet.beforeFirst();
		Utils.assertEquals(rowSet, actual);
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
