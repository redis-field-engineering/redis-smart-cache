package com.redis.sidecar.core;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.sidecar.AbstractSidecarTests;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class ConfigUpdaterTests extends AbstractSidecarTests {

	@ParameterizedTest
	@RedisTestContextsSource
	void testDriver(RedisTestContext redis) throws SQLException, ClassNotFoundException, JsonProcessingException {
		String key = "myjson";
		Config config = new Config();
		config.getRedis().setUri(redis.getRedisURI());
		config.getRedis().setCluster(redis.isCluster());
		try (ConfigUpdater updater = new ConfigUpdater(redis.getConnection(), key, config)) {
			long refreshRate = 1000;
			updater.schedule(refreshRate);
			int bufferSize = 123456890;
			redis.sync().jsonSet(key, ".bufferSize", String.valueOf(bufferSize));
			Awaitility.await().atMost(bufferSize * 2, TimeUnit.MILLISECONDS)
					.until(() -> config.getBufferSize() == bufferSize);
		}
	}

}
