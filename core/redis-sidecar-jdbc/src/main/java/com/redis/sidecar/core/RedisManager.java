package com.redis.sidecar.core;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.sidecar.core.config.Pool;
import com.redis.sidecar.core.config.Redis;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

public class RedisManager {

	private final Map<RedisURI, AbstractRedisClient> clients = new HashMap<>();
	private final Map<RedisURI, GenericObjectPool<StatefulConnection<String, ResultSet>>> pools = new HashMap<>();

	public AbstractRedisClient getClient(Redis redis) {
		RedisURI uri = redis.redisURI();
		if (!clients.containsKey(uri)) {
			clients.put(uri,
					redis.isCluster() ? RedisModulesClusterClient.create(uri) : RedisModulesClient.create(uri));
		}
		return clients.get(uri);
	}

	public GenericObjectPool<StatefulConnection<String, ResultSet>> getConnectionPool(Redis redis,
			RedisCodec<String, ResultSet> codec) {
		RedisURI uri = redis.redisURI();
		if (!pools.containsKey(uri)) {
			AbstractRedisClient client = getClient(redis);
			boolean cluster = redis.isCluster();
			GenericObjectPool<StatefulConnection<String, ResultSet>> pool = ConnectionPoolSupport
					.createGenericObjectPool(() -> cluster ? ((RedisModulesClusterClient) client).connect(codec)
							: ((RedisModulesClient) client).connect(codec), poolConfig(redis.getPool()));
			pools.put(uri, pool);
		}
		return pools.get(uri);
	}

	private GenericObjectPoolConfig<StatefulConnection<String, ResultSet>> poolConfig(Pool pool) {
		GenericObjectPoolConfig<StatefulConnection<String, ResultSet>> config = new GenericObjectPoolConfig<>();
		config.setMaxTotal(pool.getMaxActive());
		config.setMaxIdle(pool.getMaxIdle());
		config.setMinIdle(pool.getMinIdle());
		config.setTimeBetweenEvictionRuns(Duration.ofMillis(pool.getTimeBetweenEvictionRuns()));
		config.setMaxWait(Duration.ofMillis(pool.getMaxWait()));
		return config;
	}

}
