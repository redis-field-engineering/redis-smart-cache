package com.redis.smartcache;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

import io.airlift.units.DataSize;
import io.lettuce.core.SslVerifyMode;

public class BootstrapConfig {

	private RedisConfig redis = new RedisConfig();
	private DriverConfig driver = new DriverConfig();
	private Duration configStep = SmartDriver.DEFAULT_CONFIG_STEP;
	private Duration metricsStep = SmartDriver.DEFAULT_METRICS_STEP;

	public String key(String... ids) {
		return redis.key(ids);
	}

	public RedisConfig getRedis() {
		return redis;
	}

	public void setRedis(RedisConfig redis) {
		this.redis = redis;
	}

	/**
	 * 
	 * @return metrics step duration in seconds
	 */
	public Duration getMetricsStep() {
		return metricsStep;
	}

	public void setMetricsStep(Duration seconds) {
		this.metricsStep = seconds;
	}

	/**
	 * 
	 * @return config refresh step duration in seconds
	 */
	public Duration getConfigStep() {
		return configStep;
	}

	public void setConfigStep(Duration seconds) {
		this.configStep = seconds;
	}

	public DriverConfig getDriver() {
		return driver;
	}

	public void setDriver(DriverConfig driver) {
		this.driver = driver;
	}

	@Override
	public int hashCode() {
		return Objects.hash(configStep, driver, metricsStep, redis);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BootstrapConfig other = (BootstrapConfig) obj;
		return Objects.equals(configStep, other.configStep) && Objects.equals(driver, other.driver)
				&& Objects.equals(metricsStep, other.metricsStep) && Objects.equals(redis, other.redis);
	}

	public static class RedisConfig {

		private String uri;
		private boolean tls;
		private SslVerifyMode tlsVerify = SslVerifyMode.NONE;
		private boolean cluster;
		private String username;
		private char[] password;
		private String keyspace = SmartDriver.PREFIX;
		private String keySeparator = SmartDriver.DEFAULT_KEY_SEPARATOR;
		private DataSize codecBufferSize = SmartDriver.DEFAULT_BYTE_BUFFER_CAPACITY;
		private int poolSize = SmartDriver.DEFAULT_POOL_SIZE;

		public String key(String... ids) {
			StringBuilder builder = new StringBuilder(keyspace);
			for (String id : ids) {
				builder.append(keySeparator).append(id);
			}
			return builder.toString();
		}

		public String getKeyspace() {
			return keyspace;
		}

		public void setKeyspace(String keyspace) {
			this.keyspace = keyspace;
		}

		public String getUri() {
			return uri;
		}

		public void setUri(String uri) {
			this.uri = uri;
		}

		public boolean isTls() {
			return tls;
		}

		public void setTls(boolean tls) {
			this.tls = tls;
		}

		public String getKeySeparator() {
			return keySeparator;
		}

		public void setKeySeparator(String keySeparator) {
			this.keySeparator = keySeparator;
		}

		public String getUsername() {
			return username;
		}

		public void setUsername(String username) {
			this.username = username;
		}

		public char[] getPassword() {
			return password;
		}

		public void setPassword(char[] password) {
			this.password = password;
		}

		public SslVerifyMode getTlsVerify() {
			return tlsVerify;
		}

		public void setTlsVerify(SslVerifyMode sslVerifyMode) {
			this.tlsVerify = sslVerifyMode;
		}

		public boolean isCluster() {
			return cluster;
		}

		public void setCluster(boolean cluster) {
			this.cluster = cluster;
		}

		/**
		 * 
		 * @return max byte buffer capacity in bytes
		 */
		public DataSize getCodecBufferSize() {
			return codecBufferSize;
		}

		public void setCodecBufferSize(DataSize size) {
			this.codecBufferSize = size;
		}

		public void setCodecBufferSizeInBytes(long size) {
			this.codecBufferSize = DataSize.ofBytes(size);
		}

		public int getPoolSize() {
			return poolSize;
		}

		public void setPoolSize(int poolSize) {
			this.poolSize = poolSize;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + Arrays.hashCode(password);
			result = prime * result + Objects.hash(cluster, codecBufferSize, keySeparator, keyspace, poolSize, tls,
					tlsVerify, uri, username);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			RedisConfig other = (RedisConfig) obj;
			return cluster == other.cluster && Objects.equals(codecBufferSize, other.codecBufferSize)
					&& Objects.equals(keySeparator, other.keySeparator) && Objects.equals(keyspace, other.keyspace)
					&& Arrays.equals(password, other.password) && poolSize == other.poolSize && tls == other.tls
					&& tlsVerify == other.tlsVerify && Objects.equals(uri, other.uri)
					&& Objects.equals(username, other.username);
		}

	}

	public static class DriverConfig {

		private String className;
		private String url;

		public String getClassName() {
			return className;
		}

		public void setClassName(String className) {
			this.className = className;
		}

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		@Override
		public int hashCode() {
			return Objects.hash(className, url);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			DriverConfig other = (DriverConfig) obj;
			return Objects.equals(className, other.className) && Objects.equals(url, other.url);
		}

	}

}
