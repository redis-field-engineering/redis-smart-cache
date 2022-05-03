package com.redis.sidecar;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetCache extends AutoCloseable {

	ResultSet get(String sql) throws SQLException;

	void set(String sql, ResultSet resultSet) throws SQLException;

	long getMisses();

	long getHits();

}
