package com.redis.smartcache.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

public interface ResultSetCache extends AutoCloseable {

	/**
	 * 
	 * @param sql SQL statement to get the ResultSet for.
	 * @return CacheHandle that was retrieved from cache.
	 */
	CachedResultSet get(String sql);

	CachedResultSet get(String sql, Collection<String> parameters);

	CachedResultSet get(CachedResultSet cachedResultSet, Executable<ResultSet> executable) throws SQLException;

	CachedResultSet computeIfAbsent(String sql, Executable<ResultSet> executable) throws SQLException;

	CachedResultSet computeIfAbsent(String sql, Collection<String> parameters, Executable<ResultSet> executable)
			throws SQLException;

	@Override
	void close();

}
