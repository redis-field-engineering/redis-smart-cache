package com.redis.sidecar.core;

import java.sql.SQLException;
import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.sidecar.core.config.Config;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class ConfigManagerTests extends AbstractSidecarTests {

	@ParameterizedTest
	@RedisTestContextsSource
	void testDriver(RedisTestContext redis) throws SQLException, ClassNotFoundException, JsonProcessingException {
		Config config = new Config();
		String key = "myconfig";
		try (ConfigManager updater = new ConfigManager()) {
			updater.create(redis.getClient(), key, config, Duration.ofMillis(100));
			Awaitility.await().until(() -> redis.sync().jsonGet(key) != null);
			int bufferSize = 123456890;
			redis.sync().jsonSet(key, ".redis.bufferSize", String.valueOf(bufferSize));
			Awaitility.await().until(() -> config.getRedis().getBufferSize() == bufferSize);
		}
	}

}
