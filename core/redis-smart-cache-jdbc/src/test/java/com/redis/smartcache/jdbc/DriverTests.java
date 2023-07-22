package com.redis.smartcache.jdbc;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.smartcache.Driver;
import com.redis.smartcache.core.Mappers;
import com.redis.smartcache.core.config.Config;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

class DriverTests extends AbstractTests {

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
        Assertions.assertEquals(Driver.PROPERTY_URL, infos[0].name);
        Assertions.assertEquals(Driver.PROPERTY_CLASS_NAME, infos[1].name);
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
        Properties info = Mappers.properties(config);
        Assertions.assertThrows(SQLException.class, () -> driver.connect(url, info));
    }

    private String jdbcUrl(String redisURI) {
        return "jdbc:" + redisURI;
    }

    @Test
    void configProperties() throws IOException {
        String propertyName = Mappers.PROPERTY_PREFIX + ".cache.codec-buffer-capacity";
        Config config = new Config();
        DataSize bufferSize = DataSize.of(123, Unit.KILOBYTE);
        config.getCache().setCodecBufferCapacity(bufferSize);
        Properties properties = Mappers.properties(config);
        Assertions.assertEquals(bufferSize, DataSize.valueOf(properties.getProperty(propertyName)));
        Config actual = Driver.config(properties);
        Assertions.assertEquals(config, actual);
        properties.setProperty(propertyName, "10MB");
        Assertions.assertEquals(DataSize.of(10, Unit.MEGABYTE), Driver.config(properties).getCache().getCodecBufferCapacity());
    }

}
