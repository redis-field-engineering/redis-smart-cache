package com.redis.sidecar.core;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class ConfigUpdaterTests extends AbstractSidecarTests {

	@ParameterizedTest
	@RedisTestContextsSource
	void testDriver(RedisTestContext redis) throws SQLException, ClassNotFoundException, JsonProcessingException {
		Config config = new Config();
		config.setRefreshRate(1);
		config.getRedis().setUri(redis.getRedisURI());
		config.getRedis().setCluster(redis.isCluster());
		try (ConfigUpdater updater = new ConfigUpdater(redis.getConnection(), config)) {
			Awaitility.await().until(() -> redis.sync().jsonGet(updater.key()) != null);
			int bufferSize = 123456890;
			redis.sync().jsonSet(updater.key(), ".bufferSize", String.valueOf(bufferSize));
			Awaitility.await().atMost(bufferSize * 2, TimeUnit.MILLISECONDS)
					.until(() -> config.getBufferSize() == bufferSize);
		}
	}

}
