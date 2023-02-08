package com.redis.smartcache;

import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class DriverTests extends AbstractTestcontainersRedisTestBase {

	private final RedisStackContainer redis = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(redis);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDriver(RedisTestContext redis) throws SQLException, ClassNotFoundException {
		Class.forName(Driver.class.getName());
		java.sql.Driver driver = DriverManager.getDriver("jdbc:" + redis.getRedisURI());
		Assert.assertNotNull(driver);
		Assert.assertTrue(driver.getMajorVersion() >= 0);
		Assert.assertTrue(driver.getMinorVersion() >= 0);
		Assert.assertNotNull(driver.getParentLogger());
		Assert.assertFalse(driver.jdbcCompliant());
		DriverPropertyInfo[] infos = driver.getPropertyInfo(null, new Properties());
		Assert.assertNotNull(infos);
		Assert.assertEquals(2, infos.length);
		Assert.assertEquals(Driver.PROPERTY_PREFIX_DRIVER + ".url", infos[0].name);
		Assert.assertEquals(Driver.PROPERTY_PREFIX_DRIVER + ".class-name", infos[1].name);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void configUpdated(RedisTestContext redis) throws Exception {
		try (ConfigManager<RulesetConfig> configManager = new ConfigManager<>(redis.getConnection(),
				Duration.ofMillis(100))) {
			String key = "testUpdate";
			RulesetConfig config = new RulesetConfig();
			configManager.register(key, config);
			Assertions.assertNotNull(redis.sync().jsonGet(key));
			long ttl = 123;
			redis.sync().jsonSet(key, ".rules[0].ttl", String.valueOf(ttl));
			Awaitility.await().timeout(Duration.ofMillis(300))
					.until(() -> config.getRules().size() == 1 && config.getRules().get(0).getTtl() == ttl);
		}
	}

}
