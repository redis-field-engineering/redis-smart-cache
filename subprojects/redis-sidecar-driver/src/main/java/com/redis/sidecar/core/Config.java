package com.redis.sidecar.core;

import java.util.Properties;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.internal.LettuceAssert;

public class Config {

	public static final String PROPERTY_DRIVER_URL = "sidecar.driver.url";
	public static final String PROPERTY_DRIVER_CLASS = "sidecar.driver.class";
	public static final String PROPERTY_REDIS_URI = "sidecar.redis.uri";
	public static final String PROPERTY_REDIS_CLUSTER = "sidecar.redis.cluster";
	public static final String PROPERTY_REDIS_POOL_SIZE = "sidecar.redis.pool";
	public static final String PROPERTY_BYTE_BUFFER_SIZE = "sidecar.buffer.bytes";
	private static final String TRUE = String.valueOf(true);
	public static final int MEGA_BYTES = 1024 * 1024;
	public static final int DEFAULT_BYTE_BUFFER_SIZE = 100 * MEGA_BYTES;
	public static final boolean DEFAULT_REDIS_CLUSTER = false;
	public static final String DEFAULT_REDIS_URI = "redis://localhost:6379";

	private String driverClass;
	private String driverURL;
	private String redisURI;
	private boolean redisCluster;
	private int poolSize;
	private int byteBufferSize;

	public Config() {
	}

	private Config(Builder builder) {
		this.driverClass = builder.driverClass;
		this.driverURL = builder.driverURL;
		this.redisURI = builder.redisURI;
		this.redisCluster = builder.redisCluster;
		this.poolSize = builder.poolSize;
		this.byteBufferSize = builder.byteBufferSize;
	}

	public String getRedisURI() {
		return redisURI;
	}

	public void setRedisURI(String redisURI) {
		this.redisURI = redisURI;
	}

	public boolean isRedisCluster() {
		return redisCluster;
	}

	public void setRedisCluster(boolean cluster) {
		this.redisCluster = cluster;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public String getDriverClass() {
		return driverClass;
	}

	public void setDriverClass(String driverClass) {
		this.driverClass = driverClass;
	}

	public String getDriverURL() {
		return driverURL;
	}

	public void setDriverURL(String driverURL) {
		this.driverURL = driverURL;
	}

	public int getByteBufferSize() {
		return byteBufferSize;
	}

	public void setByteBufferSize(int byteBufferSize) {
		this.byteBufferSize = byteBufferSize;
	}

	public static Config load(Properties info) {
		Config config = new Config();
		config.setDriverClass(info.getProperty(PROPERTY_DRIVER_CLASS));
		config.setDriverURL(info.getProperty(PROPERTY_DRIVER_URL));
		config.setRedisURI(info.getProperty(PROPERTY_REDIS_URI, DEFAULT_REDIS_URI));
		config.setRedisCluster(getBoolean(info, PROPERTY_REDIS_CLUSTER, DEFAULT_REDIS_CLUSTER));
		config.setPoolSize(getInt(info, PROPERTY_REDIS_POOL_SIZE, GenericObjectPoolConfig.DEFAULT_MAX_TOTAL));
		config.setByteBufferSize(getInt(info, PROPERTY_BYTE_BUFFER_SIZE, DEFAULT_BYTE_BUFFER_SIZE));
		return config;
	}

	private static boolean getBoolean(Properties properties, String name, boolean defaultValue) {
		return properties.getProperty(name, String.valueOf(defaultValue)).equalsIgnoreCase(TRUE);
	}

	private static int getInt(Properties properties, String name, int defaultValue) {
		return Integer.parseInt(properties.getProperty(name, String.valueOf(defaultValue)));
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private String driverClass;
		private String driverURL;
		private String redisURI = DEFAULT_REDIS_URI;
		private boolean redisCluster = DEFAULT_REDIS_CLUSTER;
		private int poolSize = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
		private int byteBufferSize = DEFAULT_BYTE_BUFFER_SIZE;

		private Builder() {
		}

		public Builder driverClass(String driverClass) {
			LettuceAssert.notNull(driverClass, "Driver class must not be null");
			this.driverClass = driverClass;
			return this;
		}

		public Builder driverURL(String driverURL) {
			LettuceAssert.notNull(driverURL, "Driver URL must not be null");
			this.driverURL = driverURL;
			return this;
		}

		public Builder redisURI(String redisURI) {
			LettuceAssert.notNull(redisURI, "Redis URI must not be null");
			this.redisURI = redisURI;
			return this;
		}

		public Builder redisCluster(boolean redisCluster) {
			this.redisCluster = redisCluster;
			return this;
		}

		public Builder poolSize(int poolSize) {
			this.poolSize = poolSize;
			return this;
		}

		public Builder byteBufferSize(int byteBufferSize) {
			this.byteBufferSize = byteBufferSize;
			return this;
		}

		public Config build() {
			return new Config(this);
		}
	}

}
