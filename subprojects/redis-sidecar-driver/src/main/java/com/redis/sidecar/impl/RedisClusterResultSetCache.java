package com.redis.sidecar.impl;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

public class RedisClusterResultSetCache extends AbstractRedisResultSetCache {

	private final RedisClusterClient client;

	public RedisClusterResultSetCache(RedisClusterClient client,
			GenericObjectPoolConfig<StatefulConnection<String, byte[]>> poolConfig) {
		super(() -> client.connect(REDIS_CODEC), poolConfig,
				c -> ((StatefulRedisClusterConnection<String, byte[]>) c).sync());
		this.client = client;
	}

	@Override
	protected void doClose() {
		client.shutdown();
		client.getResources().shutdown();
	}

}
