package com.redis.sidecar.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public interface ResultSetCache extends AutoCloseable {

	/**
	 * 
	 * @param key the key to get the ResultSet for.
	 * @return optional with the ResultSet that was retrieved from cache or empty
	 *         optional if none found.
	 * @throws SQLException if the ResultSet could not be retrieved
	 */
	Optional<ResultSet> get(String key);

	/**
	 * Adds a ResultSet to the cache.
	 *
	 * @param key       the key to store the ResultSet under.
	 * @param seconds   key expiration in seconds.
	 * @param resultSet the ResultSet to store under the key.
	 * @throws SQLException if an error occurred while storing the ResultSet
	 */
	void put(String key, long ttl, ResultSet resultSet);

}
