package com.redis.sidecar;

import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;

import com.redis.sidecar.core.Config;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

class DriverTests extends AbstractSidecarTests {

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
		Assert.assertEquals(Config.PROPERTY_DRIVER_URL, infos[0].name);
		Assert.assertEquals(Config.PROPERTY_DRIVER_CLASS, infos[1].name);
	}

}
