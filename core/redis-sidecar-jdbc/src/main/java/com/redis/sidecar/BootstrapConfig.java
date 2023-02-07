package com.redis.sidecar;

import java.time.Duration;

import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.sidecar.codec.ResultSetCodec;

import io.lettuce.core.SslVerifyMode;

public class BootstrapConfig {

	public static final String DEFAULT_KEYSPACE = "sidecar";
	public static final String DEFAULT_KEY_SEPARATOR = ":";
	public static final Duration DEFAULT_CONFIG_STEP = Duration.ofSeconds(10);
	public static final Duration DEFAULT_METRICS_STEP = Duration.ofSeconds(60);

	private RedisConfig redis = new RedisConfig();
	private DriverConfig driver = new DriverConfig();
	private long configStep = DEFAULT_CONFIG_STEP.toSeconds();
	private long metricsStep = DEFAULT_METRICS_STEP.toSeconds();

	public String key(String id) {
		return redis.key(id);
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
	public long getMetricsStep() {
		return metricsStep;
	}

	public void setMetricsStep(long seconds) {
		this.metricsStep = seconds;
	}

	/**
	 * 
	 * @return config refresh step duration in seconds
	 */
	public long getConfigStep() {
		return configStep;
	}

	public void setConfigStep(long seconds) {
		this.configStep = seconds;
	}

	public DriverConfig getDriver() {
		return driver;
	}

	public void setDriver(DriverConfig driver) {
		this.driver = driver;
	}

	public static class RedisConfig {

		private String uri;
		private boolean tls;
		private SslVerifyMode tlsVerify = SslVerifyMode.NONE;
		private boolean cluster;
		private String username;
		private char[] password;
		private String keyspace = DEFAULT_KEYSPACE;
		private String keySeparator = DEFAULT_KEY_SEPARATOR;
		private int codecBufferSize = ResultSetCodec.DEFAULT_BYTE_BUFFER_CAPACITY;
		private PoolConfig pool = new PoolConfig();

		public String key(String id) {
			return keyspace + keySeparator + id;
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
		public int getCodecBufferSize() {
			return codecBufferSize;
		}

		public void setCodecBufferSize(int sizeInBytes) {
			this.codecBufferSize = sizeInBytes;
		}

		public PoolConfig getPool() {
			return pool;
		}

		public void setPool(PoolConfig pool) {
			this.pool = pool;
		}
	}

	public static class PoolConfig {

		public static final int DEFAULT_MAX_IDLE = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
		public static final int DEFAULT_MIN_IDLE = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
		public static final int DEFAULT_MAX_TOTAL = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
		public static final Duration DEFAULT_MAX_WAIT = BaseObjectPoolConfig.DEFAULT_MAX_WAIT;
		public static final Duration DEFAULT_TIME_BETWEEN_EVICTION_RUNS = BaseObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS;

		/**
		 * Maximum number of "idle" connections in the pool. Use a negative value to
		 * indicate an unlimited number of idle connections.
		 */
		private int maxIdle = DEFAULT_MAX_IDLE;

		/**
		 * Target for the minimum number of idle connections to maintain in the pool.
		 * This setting only has an effect if both it and time between eviction runs are
		 * positive.
		 */
		private int minIdle = DEFAULT_MIN_IDLE;

		/**
		 * Maximum number of connections that can be allocated by the pool at a given
		 * time. Use a negative value for no limit.
		 */
		private int maxActive = DEFAULT_MAX_TOTAL;

		/**
		 * Maximum amount of time a connection allocation should block before throwing
		 * an exception when the pool is exhausted. Use a negative value to block
		 * indefinitely.
		 */
		private long maxWait = DEFAULT_MAX_WAIT.toMillis();

		/**
		 * Time between runs of the idle object evictor thread. When positive, the idle
		 * object evictor thread starts, otherwise no idle object eviction is performed.
		 */
		private long timeBetweenEvictionRuns = DEFAULT_TIME_BETWEEN_EVICTION_RUNS.toMillis();

		public int getMaxIdle() {
			return this.maxIdle;
		}

		public void setMaxIdle(int maxIdle) {
			this.maxIdle = maxIdle;
		}

		public int getMinIdle() {
			return this.minIdle;
		}

		public void setMinIdle(int minIdle) {
			this.minIdle = minIdle;
		}

		public int getMaxActive() {
			return this.maxActive;
		}

		public void setMaxActive(int maxActive) {
			this.maxActive = maxActive;
		}

		public long getMaxWait() {
			return this.maxWait;
		}

		public void setMaxWait(long maxWait) {
			this.maxWait = maxWait;
		}

		public long getTimeBetweenEvictionRuns() {
			return this.timeBetweenEvictionRuns;
		}

		public void setTimeBetweenEvictionRuns(long timeBetweenEvictionRuns) {
			this.timeBetweenEvictionRuns = timeBetweenEvictionRuns;
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

	}

}
