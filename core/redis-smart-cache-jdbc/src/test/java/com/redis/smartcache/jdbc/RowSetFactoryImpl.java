package com.redis.smartcache.jdbc;

import java.sql.SQLException;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.FilteredRowSet;
import javax.sql.rowset.JdbcRowSet;
import javax.sql.rowset.JoinRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.WebRowSet;

import com.redis.smartcache.jdbc.rowset.CachedRowSetImpl;

public class RowSetFactoryImpl implements RowSetFactory {

	private static final String NOT_IMPLEMENTED = "Not implemented";

	@Override
	public CachedRowSet createCachedRowSet() throws SQLException {
		return new CachedRowSetImpl();
	}

	@Override
	public FilteredRowSet createFilteredRowSet() throws SQLException {
		throw new UnsupportedOperationException(NOT_IMPLEMENTED);
	}

	@Override
	public JdbcRowSet createJdbcRowSet() throws SQLException {
		throw new UnsupportedOperationException(NOT_IMPLEMENTED);
	}

	@Override
	public JoinRowSet createJoinRowSet() throws SQLException {
		throw new UnsupportedOperationException(NOT_IMPLEMENTED);
	}

	@Override
	public WebRowSet createWebRowSet() throws SQLException {
		throw new UnsupportedOperationException(NOT_IMPLEMENTED);
	}

}
