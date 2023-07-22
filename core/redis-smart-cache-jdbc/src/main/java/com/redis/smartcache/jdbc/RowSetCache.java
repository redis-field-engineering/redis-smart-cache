package com.redis.smartcache.jdbc;

import javax.sql.RowSet;

public interface RowSetCache extends AutoCloseable {

    /**
     * 
     * @param key the unique key to get the ResultSet for.
     * @return RowSet that was retrieved from cache or null if none
     */
    RowSet get(String key);

    void put(String key, RowSet rowSet, long ttlMillis);

}
