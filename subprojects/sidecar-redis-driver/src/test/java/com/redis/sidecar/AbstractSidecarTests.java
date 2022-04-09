package com.redis.sidecar;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import javax.sql.DataSource;

import org.testcontainers.containers.JdbcDatabaseContainer;

import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public abstract class AbstractSidecarTests extends AbstractTestcontainersRedisTestBase {

	private final RedisContainer redis = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG)).withKeyspaceNotifications();
//	private final RedisClusterContainer redisCluster = new RedisClusterContainer(
//			RedisClusterContainer.DEFAULT_IMAGE_NAME.withTag(RedisClusterContainer.DEFAULT_TAG))
//					.withKeyspaceNotifications();

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(redis);
	}

	protected static DataSource getDataSource(JdbcDatabaseContainer<?> container) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(container.getJdbcUrl());
		config.setUsername(container.getUsername());
		config.setPassword(container.getPassword());
		config.setDriverClassName(container.getDriverClassName());
		return new HikariDataSource(config);
	}

	protected static DataSource getSidecarDataSource(JdbcDatabaseContainer<?> dbContainer, RedisTestContext context) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(SidecarDriver.JDBC_URL_PREFIX + context.getServer().getRedisURI());
		Properties properties = new Properties();
		properties.setProperty(SidecarDriver.DRIVER_CLASS, dbContainer.getDriverClassName());
		properties.setProperty(SidecarDriver.DRIVER_URL, dbContainer.getJdbcUrl());
		config.setDataSourceProperties(properties);
		config.setUsername(dbContainer.getUsername());
		config.setPassword(dbContainer.getPassword());
		config.setDriverClassName(SidecarDriver.class.getName());
		return new HikariDataSource(config);
	}

}
