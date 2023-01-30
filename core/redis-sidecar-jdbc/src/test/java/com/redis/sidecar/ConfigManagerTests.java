package com.redis.sidecar;

import java.util.Arrays;
import java.util.Collection;

import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class ConfigManagerTests extends AbstractTestcontainersRedisTestBase {

	private final RedisStackContainer redis = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(redis);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDriver(RedisTestContext redis) throws Exception {
		Config config = new Config();
		config.getRedis().setUri(redis.getRedisURI());
		config.getRedis().setCluster(redis.isCluster());
		config.setRefreshRate(1);
		try (StatefulRedisModulesConnection<String, String> connection = redis.getConnection();
				ConfigManager configManager = new ConfigManager()) {
			String key = configManager.key(config);
			configManager.getConfig(connection, config);
			Awaitility.await().until(() -> redis.sync().jsonGet(key) != null);
			int bufferSize = 123456890;
			redis.sync().jsonSet(key, ".bufferSize", String.valueOf(bufferSize));
			Awaitility.await().until(() -> config.getBufferSize() == bufferSize);
		}
	}

}
