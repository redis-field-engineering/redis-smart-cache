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

public class ResultSetCodec {

	public ResultSet decode(byte[] bytes) throws SQLException, IOException {
		DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
		try {
			return new CachedResultSet(input);
		} catch (IOException e) {
			input.close();
			throw e;
		}
	}

	public byte[] encode(ResultSet resultSet) throws SQLException, IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream out = new DataOutputStream(baos)) {
			encode(out, resultSet);
			return baos.toByteArray();
		}
	}

	public Object readRow(int type, DataInputStream input) throws IOException {
		boolean wasNull = !input.readBoolean();
		if (wasNull)
			return null;
		switch (type) {
		case Types.BIT:
		case Types.BOOLEAN:
			return input.readBoolean();
		case Types.TINYINT:
			return input.readByte();
		case Types.SMALLINT:
//			return input.readShort();
		case Types.INTEGER:
			return input.readInt();
		case Types.BIGINT:
			return input.readLong();
		case Types.FLOAT:
		case Types.REAL:
			return input.readFloat();
		case Types.DOUBLE:
		case Types.NUMERIC:
		case Types.DECIMAL:
			return input.readDouble();
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGNVARCHAR:
			return input.readUTF();
		case Types.DATE:
			return new java.sql.Date(input.readLong());
		case Types.TIME:
		case Types.TIME_WITH_TIMEZONE:
			return new java.sql.Time(input.readLong());
		case Types.TIMESTAMP:
		case Types.TIMESTAMP_WITH_TIMEZONE:
			return new java.sql.Timestamp(input.readLong());
		case Types.ROWID:
			return input.readUTF();
		case Types.CLOB:
			return input.readUTF();
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
		default:
			throw new IOException("Column type no supported: " + type);
		}
	}

	private void encode(DataOutputStream out, ResultSet resultSet) throws SQLException, IOException {
		ResultSetMetaData metadata = resultSet.getMetaData();
		int columnCount = metadata.getColumnCount();
		out.writeInt(columnCount);
		for (int index = 1; index <= columnCount; index++) {
			out.writeUTF(metadata.getCatalogName(index));
			out.writeUTF(metadata.getColumnClassName(index));
			out.writeUTF(metadata.getColumnLabel(index));
			out.writeUTF(metadata.getColumnName(index));
			out.writeUTF(metadata.getColumnTypeName(index));
			out.writeInt(metadata.getColumnType(index));
			out.writeInt(metadata.getColumnDisplaySize(index));
			out.writeInt(metadata.getPrecision(index));
			out.writeUTF(metadata.getTableName(index));
			out.writeInt(metadata.getScale(index));
			out.writeUTF(metadata.getSchemaName(index));
			out.writeBoolean(metadata.isAutoIncrement(index));
			out.writeBoolean(metadata.isCaseSensitive(index));
			out.writeBoolean(metadata.isCurrency(index));
			out.writeBoolean(metadata.isDefinitelyWritable(index));
			out.writeInt(metadata.isNullable(index));
			out.writeBoolean(metadata.isReadOnly(index));
			out.writeBoolean(metadata.isSearchable(index));
			out.writeBoolean(metadata.isSigned(index));
			out.writeBoolean(metadata.isWritable(index));
		}
		int[] types = new int[metadata.getColumnCount()];
		for (int index = 0; index < types.length; index++) {
			types[index] = metadata.getColumnType(index + 1);
		}
		int pos = 0;
		while (resultSet.next()) {
			out.writeInt(++pos);
			int index = 1;
			for (int type : types) {
				switch (type) {
				case Types.BIT:
				case Types.BOOLEAN:
					writeBoolean(index, resultSet, out);
					break;
				case Types.TINYINT:
					writeByte(index, resultSet, out);
					break;
				case Types.SMALLINT:
//					writeShort(index, resultSet, out);
//					break;
				case Types.INTEGER:
					writeInteger(index, resultSet, out);
					break;
				case Types.BIGINT:
					writeLong(index, resultSet, out);
					break;
				case Types.FLOAT:
				case Types.REAL:
					writeFloat(index, resultSet, out);
					break;
				case Types.DOUBLE:
					writeDouble(index, resultSet, out);
					break;
				case Types.NUMERIC:
				case Types.DECIMAL:
					writeBigDecimal(index, resultSet, out);
					break;
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.NCHAR:
				case Types.NVARCHAR:
				case Types.LONGNVARCHAR:
					writeString(index, resultSet, out);
					break;
				case Types.DATE:
					writeDate(index, resultSet, out);
					break;
				case Types.TIME:
				case Types.TIME_WITH_TIMEZONE:
					writeTime(index, resultSet, out);
					break;
				case Types.TIMESTAMP:
				case Types.TIMESTAMP_WITH_TIMEZONE:
					writeTimestamp(index, resultSet, out);
					break;
				case Types.ROWID:
					writeRowId(index, resultSet, out);
					break;
				case Types.CLOB:
					writeClob(index, resultSet, out);
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
					writeNull(out);
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

//	private void writeShort(int column, ResultSet resultSet, DataOutputStream output) throws SQLException, IOException {
//		short val = resultSet.getShort(column);
//		boolean wasNull = resultSet.wasNull();
//		output.writeBoolean(!wasNull);
//		if (!wasNull) {
//			output.writeShort(val);
//		}
//	}

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
			try {
				int size = Math.toIntExact(clob.length());
				output.writeUTF(size == 0 ? "" : clob.getSubString(1, size));
			} catch (ArithmeticException e) {
				throw new SQLException("CLOB is too large", e);
			}
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
