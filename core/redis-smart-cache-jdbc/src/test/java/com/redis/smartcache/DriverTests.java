package com.redis.smartcache;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;

import com.redis.smartcache.core.Config;
import com.redis.smartcache.core.Config.RulesetConfig;
import com.redis.smartcache.core.ConfigManager;
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
	void testDriver(RedisTestContext redis) throws SQLException, ClassNotFoundException, IOException {
		Class.forName(Driver.class.getName());
		java.sql.Driver driver = DriverManager.getDriver("jdbc:" + redis.getRedisURI());
		Assertions.assertNotNull(driver);
		Assertions.assertTrue(driver.getMajorVersion() >= 0);
		Assertions.assertTrue(driver.getMinorVersion() >= 0);
		Assertions.assertNotNull(driver.getParentLogger());
		Assertions.assertFalse(driver.jdbcCompliant());
		DriverPropertyInfo[] infos = driver.getPropertyInfo(null, new Properties());
		Assertions.assertNotNull(infos);
		Assertions.assertEquals(2, infos.length);
		Assertions.assertEquals(Driver.PROPERTY_PREFIX_DRIVER + ".url", infos[0].name);
		Assertions.assertEquals(Driver.PROPERTY_PREFIX_DRIVER + ".class-name", infos[1].name);
		String jdbcUrl = "jdbc:" + redis.getRedisURI();
		Assertions.assertTrue(driver.acceptsURL(jdbcUrl));
		Assertions.assertThrows(SQLException.class, () -> driver.connect(null, null));
		Assertions.assertNull(driver.connect("jdbc:asdf:", null));
		Assertions.assertNull(driver.connect("jdbc:redis:", null));
		Config config = new Config();
		config.getDriver().setUrl("jdbc:asdf://sdfsdf");
		config.getDriver().setClassName("com.asdfasdf.sdfsdfkds.Issks");
		Assertions.assertThrows(SQLException.class, () -> driver.connect(jdbcUrl, Driver.properties(config)));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void configUpdated(RedisTestContext redis) throws Exception {
		String key = "testUpdate";
		RulesetConfig config = new RulesetConfig();
		Duration interval = Duration.ofMillis(100);
		try (ConfigManager<RulesetConfig> manager = new ConfigManager<>(redis.getConnection(), key, config, interval)) {
			Assertions.assertNotNull(redis.sync().jsonGet(key));
			long ttl = 123;
			redis.sync().jsonSet(key, ".rules[0].ttl", String.valueOf(ttl));
			Awaitility.await().timeout(Duration.ofMillis(300))
					.until(() -> config.getRules().size() == 1 && config.getRules().get(0).getTtl() == ttl);
		}
	}

}
