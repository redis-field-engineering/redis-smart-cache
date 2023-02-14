package com.redis.smartcache.core;

import java.sql.SQLException;

import javax.sql.RowSet;

public interface RowSetCache {

	/**
	 * 
	 * @param id SQL id to get the ResultSet for.
	 * @return ResultSet that was retrieved from cache or null if none found.
	 */
	RowSet get(String id);

	/**
	 * Adds a ResultSet to the cache.
	 *
	 * @param id     ID of the SQL statement to store the ResultSet under.
	 * @param ttl    the key TTL in seconds.
	 * @param rowSet the ResultSet to store under the key.
	 * @throws SQLException if an error occurred while storing the ResultSet
	 */
	void put(String id, long ttl, RowSet rowSet);

}
