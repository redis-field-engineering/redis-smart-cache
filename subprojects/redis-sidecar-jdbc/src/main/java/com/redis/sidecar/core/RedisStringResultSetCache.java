package com.redis.sidecar.core;

import java.sql.ResultSet;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.api.StatefulRedisConnection;

public class RedisStringResultSetCache extends StringResultSetCache<StatefulRedisModulesConnection<String, ResultSet>> {

	public RedisStringResultSetCache(RedisModulesClient client,
			GenericObjectPoolConfig<StatefulRedisModulesConnection<String, ResultSet>> poolConfig, Config config)
			throws JsonProcessingException {
		super(() -> client.connect(new ByteArrayResultSetCodec(config.getBufferSize())), poolConfig,
				StatefulRedisConnection::sync);
	}

}
