package com.redis.sidecar;

import java.sql.SQLException;

import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class ConfigManagerTests extends AbstractSidecarTests {

	@ParameterizedTest
	@RedisTestContextsSource
	void testDriver(RedisTestContext redis) throws SQLException, ClassNotFoundException, JsonProcessingException {
		Config config = new Config();
		config.getRedis().setUri(redis.getRedisURI());
		config.getRedis().setCluster(redis.isCluster());
		config.setRefreshRate(1);
		RedisManager redisManager = new RedisManager(new MeterManager());
		try (ConfigManager configManager = new ConfigManager(redisManager)) {
			String key = configManager.key(config);
			configManager.getConfig(config);
			Awaitility.await().until(() -> redis.sync().jsonGet(key) != null);
			int bufferSize = 123456890;
			redis.sync().jsonSet(key, ".redis.bufferSize", String.valueOf(bufferSize));
			Awaitility.await().until(() -> config.getRedis().getBufferSize() == bufferSize);
		}
	}

}
