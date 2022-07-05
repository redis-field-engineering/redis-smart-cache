package com.redis.sidecar.core;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.sidecar.config.Config;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

public class RedisManager {

	private final Map<String, AbstractRedisClient> clients = new HashMap<>();
	private final Map<String, GenericObjectPool<StatefulConnection<String, ResultSet>>> pools = new HashMap<>();

	public AbstractRedisClient getClient(Config config) {
		String uri = config.getRedis().getUri();
		if (!clients.containsKey(uri)) {
			boolean cluster = config.getRedis().isCluster();
			clients.put(uri, cluster ? RedisModulesClusterClient.create(uri) : RedisModulesClient.create(uri));
		}
		return clients.get(uri);
	}

	public GenericObjectPool<StatefulConnection<String, ResultSet>> getConnectionPool(Config config,
			RedisCodec<String, ResultSet> codec) {
		String uri = config.getRedis().getUri();
		if (!pools.containsKey(uri)) {
			pools.put(uri, pool(config, codec));
		}
		return pools.get(uri);
	}

	private GenericObjectPool<StatefulConnection<String, ResultSet>> pool(Config config,
			RedisCodec<String, ResultSet> codec) {
		AbstractRedisClient client = getClient(config);
		GenericObjectPoolConfig<StatefulConnection<String, ResultSet>> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(config.getRedis().getPool().getMaxActive());
		poolConfig.setMaxIdle(config.getRedis().getPool().getMaxIdle());
		poolConfig.setMinIdle(config.getRedis().getPool().getMinIdle());
		poolConfig.setTimeBetweenEvictionRuns(
				Duration.ofMillis(config.getRedis().getPool().getTimeBetweenEvictionRuns()));
		poolConfig.setMaxWait(Duration.ofMillis(config.getRedis().getPool().getMaxWait()));
		boolean cluster = config.getRedis().isCluster();
		return ConnectionPoolSupport
				.createGenericObjectPool(() -> cluster ? ((RedisModulesClusterClient) client).connect(codec)
						: ((RedisModulesClient) client).connect(codec), poolConfig);
	}

}
