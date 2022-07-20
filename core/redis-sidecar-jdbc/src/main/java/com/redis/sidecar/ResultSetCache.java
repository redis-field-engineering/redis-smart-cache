package com.redis.sidecar;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public interface ResultSetCache {

	/**
	 * 
	 * @param sql SQL statement to get the ResultSet for.
	 * @return optional with the ResultSet that was retrieved from cache or empty
	 *         optional if none found.
	 * @throws SQLException if the ResultSet could not be retrieved
	 */
	Optional<ResultSet> get(String sql);

	/**
	 * Adds a ResultSet to the cache.
	 *
	 * @param sql       SQL statement to store the ResultSet under.
	 * @param ttl       the key TTL in seconds.
	 * @param resultSet the ResultSet to store under the key.
	 * @throws SQLException if an error occurred while storing the ResultSet
	 */
	void put(String sql, long ttl, ResultSet resultSet);

}
