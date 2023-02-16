package com.redis.smartcache;

import java.sql.SQLException;

import javax.sql.RowSet;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.redis.smartcache.core.codec.RowSetCodec;
import com.redis.smartcache.core.codec.SerializedResultSetCodec;
import com.redis.smartcache.core.rowset.CachedRowSetFactory;
import com.redis.smartcache.test.RowSetBuilder;

@TestInstance(Lifecycle.PER_CLASS)
class CodecTests {

	private static final int BYTE_BUFFER_CAPACITY = 100;

	private RowSetBuilder rowSetBuilder;

	@BeforeAll
	public void setup() throws SQLException {
		rowSetBuilder = new RowSetBuilder(new CachedRowSetFactory());
	}

	@Test
	void resultSetCodec() throws SQLException {
		RowSet rowSet = rowSetBuilder.build();
		RowSetCodec codec = new RowSetCodec(new CachedRowSetFactory(), BYTE_BUFFER_CAPACITY * 1024 * 1024);
		RowSet actual = codec.decodeValue(codec.encodeValue(rowSet));
		rowSet.beforeFirst();
		TestUtils.assertEquals(rowSet, actual);
	}

	@Test
	void serializedResultSetCodec() throws SQLException {
		RowSet rowSet = rowSetBuilder.build();
		SerializedResultSetCodec codec = new SerializedResultSetCodec(BYTE_BUFFER_CAPACITY * 1024 * 1024);
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
