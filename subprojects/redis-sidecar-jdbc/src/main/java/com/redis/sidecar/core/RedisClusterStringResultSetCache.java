package com.redis.sidecar.core;

import java.sql.ResultSet;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;

public class RedisClusterStringResultSetCache
		extends StringResultSetCache<StatefulRedisModulesClusterConnection<String, ResultSet>> {

	public RedisClusterStringResultSetCache(RedisModulesClusterClient client,
			GenericObjectPoolConfig<StatefulRedisModulesClusterConnection<String, ResultSet>> poolConfig,
			Config config) throws JsonProcessingException {
		super(() -> client.connect(new ByteArrayResultSetCodec(config.getBufferSize())), poolConfig,
				StatefulRedisModulesClusterConnection::sync);
	}

}
