package com.redis.sidecar;

import java.sql.SQLException;

import javax.sql.RowSet;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.redis.sidecar.codec.ExplicitResultSetCodec;
import com.redis.sidecar.codec.JdkSerializationResultSetCodec;

@TestInstance(Lifecycle.PER_CLASS)
class CodecTests {

	private static final int BYTE_BUFFER_CAPACITY = 100000000;

	private RowSetBuilder rowSetBuilder;

	@BeforeAll
	public void setup() throws SQLException {
		rowSetBuilder = new RowSetBuilder();
	}

	@Test
	void explicitCodec() throws SQLException {
		RowSet rowSet = rowSetBuilder.build(100, 1000);
		ExplicitResultSetCodec codec = new ExplicitResultSetCodec(rowSetBuilder.getRowSetFactory(),
				BYTE_BUFFER_CAPACITY);
		RowSet actual = codec.decodeValue(codec.encodeValue(rowSet));
		rowSet.beforeFirst();
		TestUtils.assertEquals(rowSet, actual);
	}

	@Test
	void jdkCodec() throws SQLException {
		RowSet rowSet = rowSetBuilder.build(10, 100);
		JdkSerializationResultSetCodec codec = new JdkSerializationResultSetCodec(rowSetBuilder.getRowSetFactory(),
				BYTE_BUFFER_CAPACITY);
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
