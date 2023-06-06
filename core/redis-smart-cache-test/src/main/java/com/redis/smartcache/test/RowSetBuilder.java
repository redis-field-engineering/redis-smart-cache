package com.redis.smartcache.test;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetMetaDataImpl;

public class RowSetBuilder {

	private static final JDBCType[] SUPPORTED_TYPES = { JDBCType.BIT, JDBCType.TINYINT, JDBCType.SMALLINT,
			JDBCType.INTEGER, JDBCType.BIGINT, JDBCType.FLOAT, JDBCType.REAL, JDBCType.DOUBLE, JDBCType.NUMERIC,
			JDBCType.DECIMAL, JDBCType.CHAR, JDBCType.VARCHAR, JDBCType.LONGVARCHAR, JDBCType.NCHAR, JDBCType.NVARCHAR,
			JDBCType.LONGNVARCHAR, JDBCType.DATE, JDBCType.TIME, JDBCType.TIMESTAMP };
	public static final int DEFAULT_ROW_COUNT = 1000;
	public static final int DEFAULT_COLUMN_COUNT = 10;

	private final Random random = new Random();
	private final RowSetFactory rowSetFactory;

	private int leftLimit = 48; // numeral '0'
	private int rightLimit = 122; // letter 'z'
	private int displaySize = 100;
	private int columnNameSize = 10;
	private int columnLabelSize = 5;
	private int scale = 10;
	private int precision = 10;
	private int stringSize = 1000;
	private String catalogName = "";
	private String schemaName = "myschema";
	private String tableName = "mytable";

	private List<JDBCType> types = Arrays.asList(SUPPORTED_TYPES);
	private int columnCount = DEFAULT_COLUMN_COUNT;
	private List<JDBCType> columnTypes = new ArrayList<>();
	private int rowCount = DEFAULT_ROW_COUNT;
	private Optional<RowSetMetaData> metaData = Optional.empty();
	private Optional<ColumnUpdater> columnUpdater = Optional.empty();

	public RowSetBuilder(RowSetFactory rowSetFactory) {
		this.rowSetFactory = rowSetFactory;
	}

	private class DefaultColumnUpdater implements ColumnUpdater {

		@Override
		public void update(ResultSet rowSet, int columnIndex) throws SQLException {
			rowSet.updateObject(columnIndex, value(rowSet.getMetaData().getColumnType(columnIndex)));
		}

		private Object value(int type) {
			switch (type) {
			case Types.BIT:
			case Types.BOOLEAN:
				return true;
			case Types.TINYINT:
				return (byte) 123;
			case Types.SMALLINT:
				return (short) 123;
			case Types.INTEGER:
				return 123;
			case Types.BIGINT:
				return 123456789123L;
			case Types.FLOAT:
			case Types.REAL:
				return 123123123.123123123F;
			case Types.DOUBLE:
				return 123123123123.123123123123D;
			case Types.NUMERIC:
			case Types.DECIMAL:
				return BigDecimal.valueOf(1231231232131231231.123123123);
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.LONGNVARCHAR:
				return string(stringSize);
			case Types.DATE:
				return new java.sql.Date(epochMilli());
			case Types.TIME:
				return new Time(epochMilli());
			case Types.TIMESTAMP:
				return new Timestamp(epochMilli());
			default:
				throw new IllegalArgumentException("Type not supported: " + JDBCType.valueOf(type));
			}
		}

		private long epochMilli() {
			return Instant.now().toEpochMilli();
		}
	}

	public static RowSetBuilder of(RowSetFactory rowSetFactory) {
		return new RowSetBuilder(rowSetFactory);
	}

	public RowSetBuilder columnUpdater(ColumnUpdater updater) {
		this.columnUpdater = Optional.of(updater);
		return this;
	}

	public RowSetBuilder metaData(RowSetMetaData metaData) {
		this.metaData = Optional.of(metaData);
		return this;
	}

	public RowSetBuilder rowCount(int count) {
		this.rowCount = count;
		return this;
	}

	public RowSetBuilder types(JDBCType... types) {
		this.types = Arrays.asList(types);
		return this;
	}

	public RowSetBuilder columns(JDBCType... types) {
		this.columnTypes = Arrays.asList(types);
		return this;
	}

	public RowSetBuilder columnCount(int count) {
		this.columnCount = count;
		return this;
	}

	public CachedRowSet build() throws SQLException {
		CachedRowSet rowSet = rowSetFactory.createCachedRowSet();
		RowSetMetaData rowSetMetaData = metaData.orElse(defaultMetaData());
		rowSet.setMetaData(rowSetMetaData);
		ColumnUpdater updater = columnUpdater.orElse(new DefaultColumnUpdater());
		for (int index = 0; index < rowCount; index++) {
			rowSet.moveToInsertRow();
			for (int columnIndex = 1; columnIndex <= rowSetMetaData.getColumnCount(); columnIndex++) {
				updater.update(rowSet, columnIndex);
			}
			rowSet.insertRow();
		}
		rowSet.moveToCurrentRow();
		rowSet.beforeFirst();
		return rowSet;
	}

	private RowSetMetaData defaultMetaData() throws SQLException {
		RowSetMetaDataImpl metaDataImpl = new RowSetMetaDataImpl();
		List<JDBCType> columns = columnTypes();
		metaDataImpl.setColumnCount(columns.size());
		for (int index = 0; index < columns.size(); index++) {
			int columnIndex = index + 1;
			metaDataImpl.setAutoIncrement(columnIndex, false);
			metaDataImpl.setCaseSensitive(columnIndex, true);
			metaDataImpl.setCatalogName(columnIndex, catalogName);
			metaDataImpl.setColumnDisplaySize(columnIndex, displaySize);
			metaDataImpl.setColumnLabel(columnIndex, string(columnLabelSize));
			metaDataImpl.setColumnName(columnIndex, string(columnNameSize));
			metaDataImpl.setColumnType(columnIndex, columns.get(index).getVendorTypeNumber());
			metaDataImpl.setColumnTypeName(columnIndex, columns.get(index).getName());
			metaDataImpl.setCurrency(columnIndex, false);
			metaDataImpl.setNullable(columnIndex,
					index % 3 == 0 ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable);
			metaDataImpl.setPrecision(columnIndex, precision);
			metaDataImpl.setScale(columnIndex, scale);
			metaDataImpl.setSchemaName(columnIndex, schemaName);
			metaDataImpl.setSearchable(columnIndex, false);
			metaDataImpl.setSigned(columnIndex, false);
			metaDataImpl.setTableName(columnIndex, tableName);
		}
		return metaDataImpl;

	}

	private List<JDBCType> columnTypes() {
		if (columnTypes.isEmpty()) {
			List<JDBCType> columns = new ArrayList<>();
			for (int index = 0; index < columnCount; index++) {
				columns.add(types.get(index % types.size()));
			}
			return columns;
		}
		return columnTypes;
	}

	private String string(int length) {
		return random.ints(leftLimit, rightLimit + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
				.limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
				.toString();
	}

	public static JDBCType[] supportedTypes() {
		return SUPPORTED_TYPES;
	}

}
