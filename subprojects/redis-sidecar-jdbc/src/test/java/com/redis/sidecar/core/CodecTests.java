package com.redis.sidecar.core;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.redis.sidecar.jdbc.SidecarResultSetMetaData;

class CodecTests {

	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'

	private static final int[] TYPES = { Types.BIGINT, Types.INTEGER, Types.BOOLEAN, Types.REAL, Types.TIMESTAMP,
			Types.DOUBLE, Types.VARCHAR };
	private static final int MAX_DISPLAY_SIZE = 1024;
	private static final int MIN_DISPLAY_SIZE = 5;
	private static final int MIN_COLUMN_NAME_SIZE = 5;
	private static final int MAX_COLUMN_NAME_SIZE = 20;
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
	private static final int COLUMN_COUNT = 33;
	private static final int MIN_VARCHAR_SIZE = 0;
	private static final int MAX_VARCHAR_SIZE = 3000;
	private static final int KILO = 1024;
	private static final int MEGA = KILO * KILO;
	private static final int BYTE_BUFFER_CAPACITY = 300 * MEGA;

	private final Random random = new Random();

	@Test
	void encodeResultSetTest() throws SQLException {
		ByteArrayResultSetCodec codec = new ByteArrayResultSetCodec(BYTE_BUFFER_CAPACITY);
		List<Column> columns = new ArrayList<>(COLUMN_COUNT);
		for (int index = 0; index < COLUMN_COUNT; index++) {
			Column column = new Column();
			int type = TYPES[random.nextInt(TYPES.length)];
			column.setAutoIncrement(nextBoolean());
			column.setCaseSensitive(nextBoolean());
			column.setCatalogName(CATALOG_NAME);
			column.setColumnClassName(typeClass(type).getName());
			column.setColumnDisplaySize(randomInt(MIN_DISPLAY_SIZE, MAX_DISPLAY_SIZE));
			column.setColumnLabel(string(MIN_COLUMN_LABEL_SIZE, MAX_COLUMN_LABEL_SIZE));
			column.setColumnName(string(MIN_COLUMN_NAME_SIZE, MAX_COLUMN_NAME_SIZE));
			column.setColumnType(type);
			column.setColumnTypeName(typeName(type));
			column.setCurrency(nextBoolean());
			column.setDefinitelyWritable(nextBoolean());
			column.setIsNullable(random.nextInt());
			column.setPrecision(randomInt(MIN_PRECISION, MAX_PRECISION));
			column.setReadOnly(nextBoolean());
			column.setScale(randomInt(MIN_SCALE, MAX_SCALE));
			column.setSchemaName(SCHEMA_NAME);
			column.setSearchable(nextBoolean());
			column.setSigned(nextBoolean());
			column.setTableName(TABLE_NAME);
			column.setWritable(nextBoolean());
			columns.add(column);
		}
		List<List<Object>> rows = new ArrayList<>();
		for (int index = 0; index < ROW_COUNT; index++) {
			List<Object> row = new ArrayList<>();
			for (Column column : columns) {
				row.add(value(column.getColumnType()));
			}
			rows.add(row);
		}
		SidecarResultSetMetaData metaData = new SidecarResultSetMetaData(columns);
		ListResultSet expected = new ListResultSet(metaData, rows);
		ByteBuffer bytes = codec.encodeValue(expected);
		ResultSet actual = codec.decodeValue(bytes);
		expected.beforeFirst();
		TestUtils.assertEquals(expected, actual);
	}

	private int randomInt(int min, int max) {
		return min + random.nextInt(max - min);
	}

	private boolean nextBoolean() {
		return random.nextBoolean();
	}

	private Class<?> typeClass(int type) {
		switch (type) {
		case Types.BIGINT:
			return Long.class;
		case Types.INTEGER:
			return Integer.class;
		case Types.BOOLEAN:
			return Boolean.class;
		case Types.TIMESTAMP:
			return Timestamp.class;
		case Types.DOUBLE:
			return Double.class;
		case Types.REAL:
			return Float.class;
		default:
			return String.class;
		}
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
