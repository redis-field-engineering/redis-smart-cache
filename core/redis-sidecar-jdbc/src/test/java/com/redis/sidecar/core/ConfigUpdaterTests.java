package com.redis.sidecar.core;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redis.lettucemod.json.SetMode;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class ConfigUpdaterTests extends AbstractSidecarTests {

	@ParameterizedTest
	@RedisTestContextsSource
	void testDriver(RedisTestContext redis) throws SQLException, ClassNotFoundException, JsonProcessingException {
		Config config = new Config();
		config.setRefreshRate(1);
		config.getRedis().setCluster(redis.isCluster());
		String key = config.key("config");
		ObjectMapper mapper = new ObjectMapper();
		ObjectWriter writer = mapper.writerFor(config.getClass());
		redis.sync().jsonSet(key, "$", writer.writeValueAsString(config), SetMode.NX);
		try (ConfigUpdater updater = new ConfigUpdater(redis.getConnection(), mapper.readerForUpdating(config),
				config)) {
			Awaitility.await().until(() -> redis.sync().jsonGet(key) != null);
			int bufferSize = 123456890;
			redis.sync().jsonSet(key, ".bufferSize", String.valueOf(bufferSize));
			Awaitility.await().atMost(bufferSize * 2, TimeUnit.MILLISECONDS)
					.until(() -> config.getBufferSize() == bufferSize);
		}
	}

}
