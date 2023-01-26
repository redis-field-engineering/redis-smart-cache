package com.redis.sidecar;

import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;

import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class ConfigManagerTests extends AbstractSidecarTests {

	@ParameterizedTest
	@RedisTestContextsSource
	void testDriver(RedisTestContext redis) throws Exception {
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
			redis.sync().jsonSet(key, ".bufferSize", String.valueOf(bufferSize));
			Awaitility.await().until(() -> config.getBufferSize() == bufferSize);
		}
	}

}
