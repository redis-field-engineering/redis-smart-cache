package com.redis.sidecar.impl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class CachedResultSetMetaData implements ResultSetMetaData {

	private final Column[] columns;

	public CachedResultSetMetaData(Column[] columns) {
		this.columns = columns;
	}

	public Column[] getColumns() {
		return columns;
	}

	@Override
	public int getColumnCount() throws SQLException {
		return columns.length;
	}

	private Column getColumn(int column) throws SQLException {
		if (column == 0 || column > columns.length)
			throw new SQLException("Wrong column number: " + column);
		return columns[column - 1];
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		return getColumn(column).isAutoIncrement();
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return getColumn(column).isCaseSensitive();
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		return getColumn(column).isSearchable();
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		return getColumn(column).isCurrency();
	}

	@Override
	public int isNullable(int column) throws SQLException {
		return getColumn(column).getIsNullable();
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		return getColumn(column).isSigned();
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		return getColumn(column).getDisplaySize();
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return getColumn(column).getLabel();
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return getColumn(column).getName();
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		return getColumn(column).getSchemaName();
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		return getColumn(column).getPrecision();
	}

	@Override
	public int getScale(int column) throws SQLException {
		return getColumn(column).getScale();
	}

	@Override
	public String getTableName(int column) throws SQLException {
		return getColumn(column).getTableName();
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return getColumn(column).getCatalog();
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		return getColumn(column).getType();
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return getColumn(column).getTypeName();
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		return getColumn(column).isReadOnly();
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return getColumn(column).isWritable();
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return getColumn(column).isDefinitelyWritable();
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		return getColumn(column).getClassName();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
}
