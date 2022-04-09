package com.redis.sidecar.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Optional;

import com.redis.sidecar.ResultSetCache;
import com.redis.sidecar.SidecarStatement;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;

public class RedisResultSetCache implements ResultSetCache {

	private final StatefulRedisConnection<byte[], byte[]> connection;

	public RedisResultSetCache(StatefulRedisConnection<byte[], byte[]> connection) {
		this.connection = connection;
	}

	@Override
	public Optional<ResultSet> get(SidecarStatement statement, String sql) throws SQLException {
		byte[] key = key(sql);
		byte[] value = connection.sync().get(key);
		if (value == null) {
			return Optional.empty();
		}
		return Optional.of(new CachedResultSet(statement, new DataInputStream(new ByteArrayInputStream(value))));
	}

	private byte[] key(String sql) {
		return StringCodec.UTF8.encodeKey(sql).array();
	}

	@Override
	public void set(String sql, ResultSet resultSet) throws SQLException {
		byte[] key = key(sql);
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos)) {
			writeMetadata(dos, resultSet.getMetaData());
			writeResultSet(dos, resultSet);
			byte[] value = baos.toByteArray();
			connection.sync().set(key, value);
		} catch (IOException e) {
			throw new SQLException("Could not cache ResultSet", e);
		}
	}

	private void writeMetadata(DataOutputStream output, ResultSetMetaData metadata) throws IOException, SQLException {
		int columnCount = metadata.getColumnCount();
		output.writeInt(columnCount);
		for (int i = 1; i <= columnCount; i++) {
			output.writeUTF(metadata.getCatalogName(i));
			output.writeUTF(metadata.getColumnClassName(i));
			output.writeUTF(metadata.getColumnLabel(i));
			output.writeUTF(metadata.getColumnName(i));
			output.writeUTF(metadata.getColumnTypeName(i));
			output.writeInt(metadata.getColumnType(i));
			output.writeInt(metadata.getColumnDisplaySize(i));
			output.writeInt(metadata.getPrecision(i));
			output.writeUTF(metadata.getTableName(i));
			output.writeInt(metadata.getScale(i));
			output.writeUTF(metadata.getSchemaName(i));
			output.writeBoolean(metadata.isAutoIncrement(i));
			output.writeBoolean(metadata.isCaseSensitive(i));
			output.writeBoolean(metadata.isCurrency(i));
			output.writeBoolean(metadata.isDefinitelyWritable(i));
			output.writeInt(metadata.isNullable(i));
			output.writeBoolean(metadata.isReadOnly(i));
			output.writeBoolean(metadata.isSearchable(i));
			output.writeBoolean(metadata.isSigned(i));
			output.writeBoolean(metadata.isWritable(i));
		}

	}

	private void writeResultSet(DataOutputStream output, ResultSet resultSet) throws SQLException, IOException {
		ResultSetMetaData metaData = resultSet.getMetaData();
		int[] types = new int[metaData.getColumnCount()];
		for (int i = 0; i < types.length; i++)
			types[i] = metaData.getColumnType(i + 1);
		int pos = 0;
		while (resultSet.next()) {
			output.writeInt(++pos);
			int index = 1;
			for (int type : types) {
				switch (type) {
				case Types.BIT:
				case Types.BOOLEAN:
					writeBoolean(index, resultSet, output);
					break;
				case Types.TINYINT:
					writeByte(index, resultSet, output);
					break;
				case Types.SMALLINT:
					writeShort(index, resultSet, output);
					break;
				case Types.INTEGER:
					writeInteger(index, resultSet, output);
					break;
				case Types.BIGINT:
					writeLong(index, resultSet, output);
					break;
				case Types.FLOAT:
				case Types.REAL:
					writeFloat(index, resultSet, output);
					break;
				case Types.DOUBLE:
					writeDouble(index, resultSet, output);
					break;
				case Types.NUMERIC:
				case Types.DECIMAL:
					writeBigDecimal(index, resultSet, output);
					break;
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.NCHAR:
				case Types.NVARCHAR:
				case Types.LONGNVARCHAR:
					writeString(index, resultSet, output);
					break;
				case Types.DATE:
					writeDate(index, resultSet, output);
					break;
				case Types.TIME:
				case Types.TIME_WITH_TIMEZONE:
					writeTime(index, resultSet, output);
					break;
				case Types.TIMESTAMP:
				case Types.TIMESTAMP_WITH_TIMEZONE:
					writeTimestamp(index, resultSet, output);
					break;
				case Types.ROWID:
					writeRowId(index, resultSet, output);
					break;
				case Types.CLOB:
					writeClob(index, resultSet, output);
					break;
				case Types.BINARY:
				case Types.VARBINARY:
				case Types.LONGVARBINARY:
				case Types.NULL:
				case Types.OTHER:
				case Types.JAVA_OBJECT:
				case Types.DISTINCT:
				case Types.STRUCT:
				case Types.ARRAY:
				case Types.BLOB:
				case Types.REF:
				case Types.DATALINK:
				case Types.NCLOB:
				case Types.SQLXML:
				case Types.REF_CURSOR:
					writeNull(output);
					break;
				default:
					break;
				}
				index++;
			}
		}
	}

	private void writeBoolean(int column, ResultSet resultSet, DataOutputStream output)
			throws SQLException, IOException {
		boolean val = resultSet.getBoolean(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeBoolean(val);
		}
	}

	private void writeByte(int column, ResultSet resultSet, DataOutputStream output) throws SQLException, IOException {
		byte val = resultSet.getByte(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeByte(val);
		}
	}

	private void writeShort(int column, ResultSet resultSet, DataOutputStream output) throws SQLException, IOException {
		short val = resultSet.getShort(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeShort(val);
		}
	}

	private void writeInteger(int column, ResultSet resultSet, DataOutputStream output)
			throws SQLException, IOException {
		int val = resultSet.getInt(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeInt(val);
		}
	}

	private void writeLong(int column, ResultSet resultSet, DataOutputStream output) throws SQLException, IOException {
		long val = resultSet.getLong(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeLong(val);
		}
	}

	private void writeFloat(int column, ResultSet resultSet, DataOutputStream output) throws SQLException, IOException {
		float val = resultSet.getFloat(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeFloat(val);
		}
	}

	private void writeDouble(int column, ResultSet resultSet, DataOutputStream output)
			throws SQLException, IOException {
		double val = resultSet.getDouble(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeDouble(val);
		}
	}

	private void writeString(int column, ResultSet resultSet, DataOutputStream output)
			throws SQLException, IOException {
		String val = resultSet.getString(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeUTF(val);
		}
	}

	private void writeBigDecimal(int column, ResultSet resultSet, DataOutputStream output)
			throws SQLException, IOException {
		BigDecimal val = resultSet.getBigDecimal(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeDouble(val.doubleValue());
		}
	}

	private void writeDate(int column, ResultSet resultSet, DataOutputStream output) throws SQLException, IOException {
		Date val = resultSet.getDate(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeLong(val.getTime());
		}
	}

	private void writeTime(int column, ResultSet resultSet, DataOutputStream output) throws SQLException, IOException {
		Time val = resultSet.getTime(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeLong(val.getTime());
		}
	}

	private void writeTimestamp(int column, ResultSet resultSet, DataOutputStream output)
			throws SQLException, IOException {
		Timestamp val = resultSet.getTimestamp(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeLong(val.getTime());
		}
	}

	private void writeRowId(int column, ResultSet resultSet, DataOutputStream output) throws SQLException, IOException {
		RowId val = resultSet.getRowId(column);
		boolean wasNull = resultSet.wasNull();
		output.writeBoolean(!wasNull);
		if (!wasNull) {
			output.writeUTF(val.toString());
		}
	}

	private void writeClob(int column, ResultSet resultSet, DataOutputStream output) throws SQLException, IOException {
		Clob clob = resultSet.getClob(column);
		try {
			boolean wasNull = resultSet.wasNull();
			output.writeBoolean(!wasNull);
			if (wasNull) {
				return;
			}
			// TODO Support reader for long sized string
			long size = clob.length();
			output.writeUTF(size == 0 ? "" : clob.getSubString(1, (int) clob.length()));
		} finally {
			if (clob != null) {
				try {
					clob.free();
				} catch (AbstractMethodError e) {
					// May occur with old JDBC drivers
				}
			}
		}
	}

	private void writeNull(DataOutputStream output) throws IOException {
		output.writeBoolean(false);
	}

}
