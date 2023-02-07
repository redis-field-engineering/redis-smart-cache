package com.redis.sidecar;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;

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
	void configUpdated(RedisTestContext redis) throws Exception {
		try (ConfigManager<RulesConfig> configManager = new ConfigManager<>(redis.getConnection(),
				Duration.ofMillis(100))) {
			String key = "testUpdate";
			RulesConfig config = new RulesConfig();
			configManager.register(key, config);
			Assertions.assertNotNull(redis.sync().jsonGet(key));
			long ttl = 123;
			redis.sync().jsonSet(key, ".rules[0].ttl", String.valueOf(ttl));
			Awaitility.await().timeout(Duration.ofMillis(300))
					.until(() -> config.getRules().size() == 1 && config.getRules().get(0).getTtl() == ttl);
		}
	}

}
