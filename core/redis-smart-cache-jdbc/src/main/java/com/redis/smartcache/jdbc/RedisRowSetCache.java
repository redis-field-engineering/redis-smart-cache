package com.redis.smartcache.jdbc;

import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.RowSet;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.codec.RedisCodec;

public class RedisRowSetCache implements RowSetCache {

    private static final Logger log = Logger.getLogger(RedisRowSetCache.class.getName());

    private static final String ERROR_OOM = "OOM command not allowed";

    private final StatefulRedisModulesConnection<String, RowSet> connection;

    private final long oomRetryInterval;

    private long nextOOMCheck;

    public RedisRowSetCache(AbstractRedisClient client, RedisCodec<String, RowSet> codec, Duration oomRetryInterval) {
        this.connection = RedisModulesUtils.connection(client, codec);
        this.oomRetryInterval = oomRetryInterval.toMillis();
    }

    @Override
    public RowSet get(String key) {
        if (connection.isOpen()) {
            return connection.sync().get(key);
        }
        return null;
    }

    @Override
    public void put(String key, RowSet rowSet, long ttlMillis) {
        long currentTimeMillis = System.currentTimeMillis();
        if (connection.isOpen() && currentTimeMillis > nextOOMCheck) {
            try {
                doPut(key, rowSet, ttlMillis);
            } catch (RedisException e) {
                String message = e.getMessage();
                if (message != null && message.startsWith(ERROR_OOM)) {
                    log.log(Level.SEVERE, "Redis server out of memory. Puts disabled until memory is available.", e);
                    nextOOMCheck = System.currentTimeMillis() + oomRetryInterval;
                } else {
                    log.log(Level.SEVERE, "Could not put rowset in cache", e);
                }
            }
        }
    }

    private void doPut(String key, RowSet rowSet, long ttlMillis) {
        if (ttlMillis > 0) {
            connection.sync().psetex(key, ttlMillis, rowSet);
        } else {
            connection.sync().set(key, rowSet);
        }
    }

    @Override
    public void close() {
        connection.close();
    }

}
