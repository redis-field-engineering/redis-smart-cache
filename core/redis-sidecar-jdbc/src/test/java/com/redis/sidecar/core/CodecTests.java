package com.redis.sidecar.core;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Random;

import javax.sql.RowSet;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

import org.junit.jupiter.api.Test;

import com.redis.sidecar.TestUtils;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class CodecTests {

	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'

	private static final int[] TYPES = { Types.BIGINT, Types.INTEGER, Types.BOOLEAN, Types.REAL, Types.TIMESTAMP,
			Types.DOUBLE, Types.VARCHAR };
	private static final int MAX_DISPLAY_SIZE = 1024;
	private static final int MIN_DISPLAY_SIZE = 5;
	private static final int MIN_COLUMN_NAME_SIZE = 2;
	private static final int MAX_COLUMN_NAME_SIZE = 3;
	private static final int MIN_COLUMN_LABEL_SIZE = 5;
	private static final int MAX_COLUMN_LABEL_SIZE = 20;
	private static final int MIN_SCALE = 0;
	private static final int MAX_SCALE = 10;
	private static final int MIN_PRECISION = 0;
	private static final int MAX_PRECISION = 10;
	private static final String CATALOG_NAME = "";
	private static final String SCHEMA_NAME = "myschema";
	private static final String TABLE_NAME = "mytable";
	private static final int ROW_COUNT = 12345;
	private static final int COLUMN_COUNT = 31;
	private static final int MIN_VARCHAR_SIZE = 0;
	private static final int MAX_VARCHAR_SIZE = 3000;
	private static final int KILO = 1024;
	private static final int MEGA = KILO * KILO;
	private static final int BYTE_BUFFER_CAPACITY = 300 * MEGA;

	private final Random random = new Random();

	@Test
	void encodeResultSetTest() throws SQLException {
		RowSetFactory rowSetFactory = RowSetProvider.newFactory();
		CachedRowSet rowSet = rowSetFactory.createCachedRowSet();
		RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
		metaData.setColumnCount(COLUMN_COUNT);
		for (int columnIndex = 1; columnIndex <= COLUMN_COUNT; columnIndex++) {
			int type = TYPES[random.nextInt(TYPES.length)];
			metaData.setAutoIncrement(columnIndex, nextBoolean());
			metaData.setCaseSensitive(columnIndex, nextBoolean());
			metaData.setCatalogName(columnIndex, CATALOG_NAME);
			metaData.setColumnDisplaySize(columnIndex, randomInt(MIN_DISPLAY_SIZE, MAX_DISPLAY_SIZE));
			metaData.setColumnLabel(columnIndex, string(MIN_COLUMN_LABEL_SIZE, MAX_COLUMN_LABEL_SIZE));
			metaData.setColumnName(columnIndex, string(MIN_COLUMN_NAME_SIZE, MAX_COLUMN_NAME_SIZE));
			metaData.setColumnType(columnIndex, type);
			metaData.setColumnTypeName(columnIndex, typeName(type));
			metaData.setCurrency(columnIndex, nextBoolean());
			metaData.setNullable(columnIndex, random.nextInt(ResultSetMetaData.columnNullableUnknown + 1));
			metaData.setPrecision(columnIndex, randomInt(MIN_PRECISION, MAX_PRECISION));
			metaData.setScale(columnIndex, randomInt(MIN_SCALE, MAX_SCALE));
			metaData.setSchemaName(columnIndex, SCHEMA_NAME);
			metaData.setSearchable(columnIndex, nextBoolean());
			metaData.setSigned(columnIndex, nextBoolean());
			metaData.setTableName(columnIndex, TABLE_NAME);
		}
		rowSet.setMetaData(metaData);
		for (int index = 0; index < ROW_COUNT; index++) {
			rowSet.moveToInsertRow();
			for (int columnIndex = 1; columnIndex <= COLUMN_COUNT; columnIndex++) {
				if (metaData.isNullable(columnIndex) == ResultSetMetaData.columnNullable && random.nextBoolean()) {
					rowSet.updateNull(columnIndex);
				} else {
					rowSet.updateObject(columnIndex, value(metaData.getColumnType(columnIndex)));
				}
			}
			rowSet.insertRow();
		}
		rowSet.moveToCurrentRow();
		rowSet.beforeFirst();
		rowSet.beforeFirst();
		CachedRowSet input = rowSetFactory.createCachedRowSet();
		input.populate(rowSet);
		rowSet.beforeFirst();
		ByteArrayResultSetCodec codec = new ByteArrayResultSetCodec(rowSetFactory, BYTE_BUFFER_CAPACITY,
				new SimpleMeterRegistry());
		RowSet actual = codec.decodeValue(codec.encodeValue(input));
		actual.beforeFirst();
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

	private int randomInt(int min, int max) {
		return min + random.nextInt(max - min);
	}

	private boolean nextBoolean() {
		return random.nextBoolean();
	}

	private String typeName(int type) {
		switch (type) {
		case Types.BIGINT:
			return "bigint";
		case Types.INTEGER:
			return "int";
		case Types.BOOLEAN:
			return "boolean";
		case Types.TIMESTAMP:
			return "timestamp";
		case Types.DOUBLE:
			return "double";
		case Types.REAL:
			return "real";
		default:
			return "varchar";
		}
	}

	private Object value(int type) {
		switch (type) {
		case Types.BIGINT:
			return random.nextLong();
		case Types.INTEGER:
			return random.nextInt();
		case Types.BOOLEAN:
			return random.nextBoolean();
		case Types.TIMESTAMP:
			return new Timestamp(random.nextLong());
		case Types.DOUBLE:
			return random.nextDouble();
		case Types.REAL:
			return random.nextFloat();
		default:
			return string(MIN_VARCHAR_SIZE, MAX_VARCHAR_SIZE);
		}
	}

	private String string(int min, int max) {
		int length = min + random.nextInt((max - min) + 1);
		return random.ints(LEFT_LIMIT, RIGHT_LIMIT + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
				.limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
				.toString();
	}

}
