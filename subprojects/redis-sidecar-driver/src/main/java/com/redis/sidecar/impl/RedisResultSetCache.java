package com.redis.sidecar.impl;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;

public class RedisResultSetCache extends AbstractRedisResultSetCache {

	private final RedisClient client;

	public RedisResultSetCache(RedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, byte[]>> poolConfig) {
		super(() -> client.connect(REDIS_CODEC), poolConfig, c -> ((StatefulRedisConnection<String, byte[]>) c).sync());
		this.client = client;
	}

	@Override
	protected void doClose() {
		client.shutdown();
		client.getResources().shutdown();
	}

}
