package com.redis.smartcache.core;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.smartcache.Driver;
import com.redis.testcontainers.RedisStackContainer;

@Testcontainers
class DriverTests {

	@Container
	private final RedisStackContainer redis = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Test
	void configureDriver() throws SQLException, ClassNotFoundException, IOException {
		Class.forName(Driver.class.getName());
		String redisURI = "redis://localhost:6379";
		java.sql.Driver driver = DriverManager.getDriver(jdbcUrl(redisURI));
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
		Assertions.assertTrue(driver.acceptsURL(jdbcUrl(redisURI)));
		Assertions.assertThrows(SQLException.class, () -> driver.connect(null, null));
		Assertions.assertNull(driver.connect("jdbc:asdf:", null));
		Assertions.assertNull(driver.connect("jdbc:redis:", null));
	}

	@Test
	void connect() throws IOException, SQLException {
		java.sql.Driver driver = DriverManager.getDriver(jdbcUrl(redis.getRedisURI()));
		Config config = new Config();
		config.getDriver().setUrl("jdbc:asdf://sdfsdf");
		config.getDriver().setClassName("com.asdfasdf.sdfsdfkds.Issks");
		String url = jdbcUrl(redis.getRedisURI());
		Properties info = Driver.properties(config);
		Assertions.assertThrows(SQLException.class, () -> driver.connect(url, info));

	}

	private String jdbcUrl(String redisURI) {
		return "jdbc:" + redisURI;
	}

}
