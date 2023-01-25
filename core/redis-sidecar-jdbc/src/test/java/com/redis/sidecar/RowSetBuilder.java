package com.redis.sidecar;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Random;

import javax.sql.RowSet;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

public class RowSetBuilder {

	public enum Type {
		BIGINT("bigint", Types.BIGINT), INTEGER("int", Types.BIGINT), BOOLEAN("boolean", Types.BOOLEAN),
		REAL("real", Types.REAL), TIMESTAMP("timestamp", Types.TIMESTAMP), DOUBLE("double", Types.DOUBLE),
		VARCHAR("varchar", Types.VARCHAR);

		private String name;
		private int code;

		private Type(String name, int code) {
			this.name = name;
			this.code = code;
		}

		public String getName() {
			return name;
		}

		public int getCode() {
			return code;
		}
	}

	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'

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
	private static final int MIN_VARCHAR_SIZE = 0;
	private static final int MAX_VARCHAR_SIZE = 3000;
	private static final Random RANDOM = new Random();

	private final RowSetFactory rowSetFactory;

	public RowSetBuilder() throws SQLException {
		this(RowSetProvider.newFactory());
	}

	public RowSetBuilder(RowSetFactory rowSetFactory) {
		this.rowSetFactory = rowSetFactory;
	}

	public RowSetFactory getRowSetFactory() {
		return rowSetFactory;
	}

	public CachedRowSet build(int columns, int rows) throws SQLException {
		CachedRowSet cachedRowSet = rowSetFactory.createCachedRowSet();
		RowSetMetaData metaData = metaData(columns, rows);
		cachedRowSet.setMetaData(metaData);
		for (int index = 0; index < rows; index++) {
			cachedRowSet.moveToInsertRow();
			for (int columnIndex = 1; columnIndex <= columns; columnIndex++) {
				if (metaData.isNullable(columnIndex) == ResultSetMetaData.columnNullable && RANDOM.nextBoolean()) {
					cachedRowSet.updateNull(columnIndex);
				} else {
					cachedRowSet.updateObject(columnIndex, value(metaData.getColumnType(columnIndex)));
				}
			}
			cachedRowSet.insertRow();
		}
		cachedRowSet.moveToCurrentRow();
		cachedRowSet.beforeFirst();
		cachedRowSet.beforeFirst();
		return cachedRowSet;
	}

	private RowSetMetaData metaData(int columns, int rows) throws SQLException {
		RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
		metaData.setColumnCount(columns);
		Type[] types = Type.values();
		for (int columnIndex = 1; columnIndex <= columns; columnIndex++) {
			Type type = types[RANDOM.nextInt(types.length)];
			metaData.setAutoIncrement(columnIndex, nextBoolean());
			metaData.setCaseSensitive(columnIndex, nextBoolean());
			metaData.setCatalogName(columnIndex, CATALOG_NAME);
			metaData.setColumnDisplaySize(columnIndex, randomInt(MIN_DISPLAY_SIZE, MAX_DISPLAY_SIZE));
			metaData.setColumnLabel(columnIndex, string(MIN_COLUMN_LABEL_SIZE, MAX_COLUMN_LABEL_SIZE));
			metaData.setColumnName(columnIndex, string(MIN_COLUMN_NAME_SIZE, MAX_COLUMN_NAME_SIZE));
			metaData.setColumnType(columnIndex, type.getCode());
			metaData.setColumnTypeName(columnIndex, type.getName());
			metaData.setCurrency(columnIndex, nextBoolean());
			metaData.setNullable(columnIndex, RANDOM.nextInt(ResultSetMetaData.columnNullableUnknown + 1));
			metaData.setPrecision(columnIndex, randomInt(MIN_PRECISION, MAX_PRECISION));
			metaData.setScale(columnIndex, randomInt(MIN_SCALE, MAX_SCALE));
			metaData.setSchemaName(columnIndex, SCHEMA_NAME);
			metaData.setSearchable(columnIndex, nextBoolean());
			metaData.setSigned(columnIndex, nextBoolean());
			metaData.setTableName(columnIndex, TABLE_NAME);
		}
		return metaData;
	}

	public RowSet copy(RowSet rowSet) throws SQLException {
		CachedRowSet copy = rowSetFactory.createCachedRowSet();
		rowSet.beforeFirst();
		copy.populate(rowSet);
		return copy;
	}

	private static int randomInt(int min, int max) {
		return min + RANDOM.nextInt(max - min);
	}

	private static boolean nextBoolean() {
		return RANDOM.nextBoolean();
	}

	private static Object value(int type) {
		switch (type) {
		case Types.BIGINT:
			return RANDOM.nextLong();
		case Types.INTEGER:
			return RANDOM.nextInt();
		case Types.BOOLEAN:
			return RANDOM.nextBoolean();
		case Types.TIMESTAMP:
			return new Timestamp(RANDOM.nextLong());
		case Types.DOUBLE:
			return RANDOM.nextDouble();
		case Types.REAL:
			return RANDOM.nextFloat();
		default:
			return string(MIN_VARCHAR_SIZE, MAX_VARCHAR_SIZE);
		}
	}

	private static String string(int min, int max) {
		int length = min + RANDOM.nextInt((max - min) + 1);
		return RANDOM.ints(LEFT_LIMIT, RIGHT_LIMIT + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
				.limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
				.toString();
	}
}
