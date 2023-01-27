package com.redis.sidecar.rowset;

import java.sql.SQLException;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.FilteredRowSet;
import javax.sql.rowset.JdbcRowSet;
import javax.sql.rowset.JoinRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.WebRowSet;

public class SidecarRowSetFactory implements RowSetFactory {

	@Override
	public CachedRowSet createCachedRowSet() throws SQLException {
		return new CachedRowSetImpl();
	}

	@Override
	public FilteredRowSet createFilteredRowSet() throws SQLException {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public JdbcRowSet createJdbcRowSet() throws SQLException {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public JoinRowSet createJoinRowSet() throws SQLException {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public WebRowSet createWebRowSet() throws SQLException {
		throw new UnsupportedOperationException("Not implemented");
	}

}
