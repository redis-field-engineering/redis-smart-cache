package com.redis.sidecar;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.sidecar.Config.Pool;
import com.redis.sidecar.Config.Redis;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.lettuce.core.metrics.MicrometerOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.micrometer.core.instrument.MeterRegistry;

public class RedisManager {

	private final Map<RedisURI, AbstractRedisClient> clients = new HashMap<>();
	private final Map<RedisURI, GenericObjectPool<StatefulConnection<String, ResultSet>>> pools = new HashMap<>();

	public AbstractRedisClient getClient(Redis redis, MeterRegistry meterRegistry) {
		if (!clients.containsKey(redis.redisURI())) {
			clients.put(redis.redisURI(), client(redis, meterRegistry));
		}
		return clients.get(redis.redisURI());
	}

	private AbstractRedisClient client(Redis redis, MeterRegistry meterRegistry) {
		MicrometerOptions options = MicrometerOptions.create();
		ClientResources resources = ClientResources.builder()
				.commandLatencyRecorder(new MicrometerCommandLatencyRecorder(meterRegistry, options)).build();
		if (redis.isCluster()) {
			return RedisModulesClusterClient.create(resources, redis.redisURI());
		}
		return RedisModulesClient.create(resources, redis.redisURI());
	}

	public AbstractRedisClient client(Redis redis) {
		if (redis.isCluster()) {
			return RedisModulesClusterClient.create(redis.redisURI());
		}
		return RedisModulesClient.create(redis.redisURI());
	}

	public GenericObjectPool<StatefulConnection<String, ResultSet>> getConnectionPool(Redis redis,
			RedisCodec<String, ResultSet> codec) {
		RedisURI uri = redis.redisURI();
		if (!pools.containsKey(uri)) {
			AbstractRedisClient client = clients.get(redis.redisURI());
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

	public void clear() {
		pools.forEach((k, v) -> v.close());
		pools.clear();
		clients.forEach((k, v) -> {
			v.shutdown();
			v.getResources().shutdown();
		});
		clients.clear();

	}

}
