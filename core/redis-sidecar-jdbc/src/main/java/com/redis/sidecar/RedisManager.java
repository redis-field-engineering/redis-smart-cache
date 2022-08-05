package com.redis.sidecar;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
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
import io.lettuce.core.resource.ClientResources.Builder;
import io.lettuce.core.support.ConnectionPoolSupport;

public class RedisManager {

	private final Map<RedisURI, AbstractRedisClient> clients = new HashMap<>();
	private final Map<RedisURI, GenericObjectPool<StatefulConnection<String, ResultSet>>> pools = new HashMap<>();

	private final MeterManager meterManager;

	public RedisManager(MeterManager meterManager) {
		this.meterManager = meterManager;
	}

	@SuppressWarnings("unchecked")
	public <T extends AbstractRedisClient> T getClient(Config config) {
		Redis redis = config.getRedis();
		RedisURI uri = redis.uri();
		if (clients.containsKey(uri)) {
			return (T) clients.get(uri);
		}
		Builder builder = ClientResources.builder();
		if (config.getMetrics().isLettuce()) {
			builder = builder.commandLatencyRecorder(
					new MicrometerCommandLatencyRecorder(meterManager.getRegistry(config), MicrometerOptions.create()));
		}
		ClientResources resources = builder.build();
		AbstractRedisClient client = redis.isCluster() ? RedisModulesClusterClient.create(resources, uri)
				: RedisModulesClient.create(resources, uri);
		clients.put(uri, client);
		return (T) client;
	}

	public GenericObjectPool<StatefulConnection<String, ResultSet>> getConnectionPool(Config config,
			RedisCodec<String, ResultSet> codec) {
		Redis redis = config.getRedis();
		RedisURI uri = redis.uri();
		if (pools.containsKey(uri)) {
			return pools.get(uri);
		}
		GenericObjectPool<StatefulConnection<String, ResultSet>> pool = ConnectionPoolSupport.createGenericObjectPool(
				() -> redis.isCluster() ? ((RedisModulesClusterClient) getClient(config)).connect(codec)
						: ((RedisModulesClient) getClient(config)).connect(codec),
				poolConfig(redis.getPool()));
		pools.put(uri, pool);
		return pool;
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

	public StatefulRedisModulesConnection<String, String> connection(Config config) {
		if (config.getRedis().isCluster()) {
			return ((RedisModulesClusterClient) getClient(config)).connect();
		}
		return ((RedisModulesClient) getClient(config)).connect();
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
