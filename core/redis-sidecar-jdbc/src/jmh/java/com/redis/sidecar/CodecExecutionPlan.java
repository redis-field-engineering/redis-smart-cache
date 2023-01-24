package com.redis.sidecar;

import java.io.IOException;
import java.nio.ByteBuffer;
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

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import com.redis.sidecar.codec.ExplicitResultSetCodec;
import com.redis.sidecar.codec.JdkSerializationResultSetCodec;

@State(Scope.Benchmark)
public class CodecExecutionPlan {

	private static final int LEFT_LIMIT = 48; // numeral '0'
	private static final int RIGHT_LIMIT = 122; // letter 'z'

	private static final int KILO = 1024;
	private static final int MEGA = KILO * KILO;

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
	private static final int MIN_VARCHAR_SIZE = 0;
	private static final int MAX_VARCHAR_SIZE = 3000;
	private static final int BYTE_BUFFER_CAPACITY = 300 * MEGA;
	private static final Random RANDOM = new Random();

	@Param({ "10", "100", "1000" })
	public int columns;
	@Param({ "10", "100", "1000" })
	public int rows;

	private RowSet rowSet;
	private RowSetFactory rowSetFactory;
	private ExplicitResultSetCodec explicitCodec;
	private JdkSerializationResultSetCodec jdkCodec;
	private ByteBuffer explicitByteBuffer;
	private byte[] jdkBytes;

	@Setup(Level.Trial)
	public void setUpFactory() throws SQLException {
		this.rowSetFactory = RowSetProvider.newFactory();
		this.explicitCodec = new ExplicitResultSetCodec(rowSetFactory, BYTE_BUFFER_CAPACITY);
		this.jdkCodec = new JdkSerializationResultSetCodec(rowSetFactory, BYTE_BUFFER_CAPACITY);
	}

	@Setup(Level.Invocation)
	public void setUp() throws SQLException, IOException {
		CachedRowSet cachedRowSet = rowSetFactory.createCachedRowSet();
		RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
		metaData.setColumnCount(columns);
		for (int columnIndex = 1; columnIndex <= columns; columnIndex++) {
			int type = TYPES[RANDOM.nextInt(TYPES.length)];
			metaData.setAutoIncrement(columnIndex, nextBoolean());
			metaData.setCaseSensitive(columnIndex, nextBoolean());
			metaData.setCatalogName(columnIndex, CATALOG_NAME);
			metaData.setColumnDisplaySize(columnIndex, randomInt(MIN_DISPLAY_SIZE, MAX_DISPLAY_SIZE));
			metaData.setColumnLabel(columnIndex, string(MIN_COLUMN_LABEL_SIZE, MAX_COLUMN_LABEL_SIZE));
			metaData.setColumnName(columnIndex, string(MIN_COLUMN_NAME_SIZE, MAX_COLUMN_NAME_SIZE));
			metaData.setColumnType(columnIndex, type);
			metaData.setColumnTypeName(columnIndex, typeName(type));
			metaData.setCurrency(columnIndex, nextBoolean());
			metaData.setNullable(columnIndex, RANDOM.nextInt(ResultSetMetaData.columnNullableUnknown + 1));
			metaData.setPrecision(columnIndex, randomInt(MIN_PRECISION, MAX_PRECISION));
			metaData.setScale(columnIndex, randomInt(MIN_SCALE, MAX_SCALE));
			metaData.setSchemaName(columnIndex, SCHEMA_NAME);
			metaData.setSearchable(columnIndex, nextBoolean());
			metaData.setSigned(columnIndex, nextBoolean());
			metaData.setTableName(columnIndex, TABLE_NAME);
		}
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
		this.rowSet = cachedRowSet;
		this.explicitByteBuffer = explicitCodec.encodeValue(newRowSet());
		this.jdkBytes = jdkCodec.encode(newRowSet());
	}

	public RowSet newRowSet() throws SQLException {
		CachedRowSet input = rowSetFactory.createCachedRowSet();
		rowSet.beforeFirst();
		input.populate(rowSet);
		return input;
	}

	private static int randomInt(int min, int max) {
		return min + RANDOM.nextInt(max - min);
	}

	private static boolean nextBoolean() {
		return RANDOM.nextBoolean();
	}

	private static String typeName(int type) {
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

	public ExplicitResultSetCodec getExplicitCodec() {
		return explicitCodec;
	}

	public ByteBuffer getExplicitByteBufer() {
		return explicitByteBuffer;
	}

	public JdkSerializationResultSetCodec getJdkCodec() {
		return jdkCodec;
	}

	public byte[] getJdkBytes() {
		return jdkBytes;
	}
}