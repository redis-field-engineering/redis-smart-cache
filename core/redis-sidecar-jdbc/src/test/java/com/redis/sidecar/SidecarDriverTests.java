package com.redis.sidecar;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class SidecarDriverTests extends AbstractTestcontainersRedisTestBase {

	private final RedisStackContainer redis = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(redis);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDriver(RedisTestContext redis) throws SQLException, ClassNotFoundException {
		Class.forName(SidecarDriver.class.getName());
		Driver driver = DriverManager.getDriver("jdbc:" + redis.getRedisURI());
		Assert.assertNotNull(driver);
		Assert.assertTrue(driver.getMajorVersion() >= 0);
		Assert.assertTrue(driver.getMinorVersion() >= 0);
		Assert.assertNotNull(driver.getParentLogger());
		Assert.assertFalse(driver.jdbcCompliant());
		DriverPropertyInfo[] infos = driver.getPropertyInfo(null, new Properties());
		Assert.assertNotNull(infos);
		Assert.assertEquals(2, infos.length);
		Assert.assertEquals("sidecar.driver.url", infos[0].name);
		Assert.assertEquals("sidecar.driver.class-name", infos[1].name);
	}

}
