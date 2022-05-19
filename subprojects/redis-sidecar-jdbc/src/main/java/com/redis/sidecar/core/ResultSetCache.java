package com.redis.sidecar.core;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetCache extends AutoCloseable {

	ResultSet get(String sql) throws SQLException;

	void put(String sql, ResultSet resultSet) throws SQLException;

}
