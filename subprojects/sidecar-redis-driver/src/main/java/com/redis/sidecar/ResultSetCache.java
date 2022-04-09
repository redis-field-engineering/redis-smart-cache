package com.redis.sidecar;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public interface ResultSetCache {

	Optional<ResultSet> get(SidecarStatement statement, String sql) throws SQLException;

	void set(String sql, ResultSet resultSet) throws SQLException;

}
