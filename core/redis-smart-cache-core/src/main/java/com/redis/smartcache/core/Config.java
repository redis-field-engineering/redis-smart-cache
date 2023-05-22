package com.redis.smartcache.core;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.*;
import java.util.concurrent.TimeUnit;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.lettuce.core.SslVerifyMode;


public class Config {

	public static final String DEFAULT_NAME = "smartcache";

	private String name = DEFAULT_NAME;
	private DriverConfig driver = new DriverConfig();
	private RedisConfig redis = new RedisConfig();
	private RulesetConfig ruleset = new RulesetConfig();
	private MetricsConfig metrics = new MetricsConfig();
	private AnalyzerConfig analyzer = new AnalyzerConfig();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public DriverConfig getDriver() {
		return driver;
	}

	public void setDriver(DriverConfig driver) {
		this.driver = driver;
	}

	public RulesetConfig getRuleset() {
		return ruleset;
	}

	public void setRuleset(RulesetConfig ruleset) {
		this.ruleset = ruleset;
	}

	public RedisConfig getRedis() {
		return redis;
	}

	public void setRedis(RedisConfig redis) {
		this.redis = redis;
	}

	public MetricsConfig getMetrics() {
		return metrics;
	}

	public void setMetrics(MetricsConfig metrics) {
		this.metrics = metrics;
	}

	public AnalyzerConfig getAnalyzer() {
		return analyzer;
	}

	public void setAnalyzer(AnalyzerConfig analyzer) {
		this.analyzer = analyzer;
	}

	@Override
	public int hashCode() {
		return Objects.hash(redis.getUri(), name);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Config other = (Config) obj;
		return Objects.equals(redis.getUri(), other.redis.getUri()) && Objects.equals(name, other.name);
	}

	public static class MetricsConfig {

		public enum Registry {

			JMX, SIMPLE, REDIS

		}

		public static final Duration DEFAULT_STEP = new Duration(60, TimeUnit.SECONDS);

		private boolean enabled = true;
		private Registry registry = Registry.REDIS;
		private Duration step = DEFAULT_STEP;

		/**
		 * 
		 * @return metrics publishing interval
		 */
		public Duration getStep() {
			return step;
		}

		public void setStep(Duration duration) {
			this.step = duration;
		}

		public boolean isEnabled() {
			return enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

		public Registry getRegistry() {
			return registry;
		}

		public void setRegistry(Registry registry) {
			this.registry = registry;
		}
	}

	public static class RedisConfig {

		public static final DataSize DEFAULT_BUFFER_CAPACITY = DataSize.of(10, Unit.MEGABYTE);

		private String uri;
		private boolean cluster;
		private boolean tls;
		private SslVerifyMode tlsVerify = SslVerifyMode.NONE;
		private String username;
		private char[] password;
		private DataSize codecBufferCapacity = DEFAULT_BUFFER_CAPACITY;
		private String keySeparator = KeyBuilder.DEFAULT_SEPARATOR;

		/**
		 * 
		 * @return max byte buffer capacity in bytes
		 */
		public DataSize getCodecBufferCapacity() {
			return codecBufferCapacity;
		}

		public void setCodecBufferCapacity(DataSize size) {
			this.codecBufferCapacity = size;
		}

		public void setCodecBufferSizeInBytes(long size) {
			this.codecBufferCapacity = DataSize.ofBytes(size);
		}

		public String getKeySeparator() {
			return keySeparator;
		}

		public void setKeySeparator(String keySeparator) {
			this.keySeparator = keySeparator;
		}

		public boolean isTls() {
			return tls;
		}

		public void setTls(boolean tls) {
			this.tls = tls;
		}

		public SslVerifyMode getTlsVerify() {
			return tlsVerify;
		}

		public void setTlsVerify(SslVerifyMode tlsVerify) {
			this.tlsVerify = tlsVerify;
		}

		public String getUri() {
			return uri;
		}

		public void setUri(String uri) {
			this.uri = uri;
		}

		public boolean isCluster() {
			return cluster;
		}

		public void setCluster(boolean cluster) {
			this.cluster = cluster;
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

		@Override
		public int hashCode() {
			return Objects.hash(uri);
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
			return Objects.equals(uri, other.uri);
		}

	}

	public static class AnalyzerConfig {

		public static final int DEFAULT_QUEUE_CAPACITY = 10000;
		public static final int DEFAULT_THREAD_COUNT = 1;
		public static final Duration DEFAULT_FLUSH_INTERVAL = new Duration(50, TimeUnit.MILLISECONDS);
		public static final int DEFAULT_BATCH_SIZE = 50;
		public static final int DEFAULT_CACHE_CAPACITY = 10000;

		private int cacheCapacity = DEFAULT_CACHE_CAPACITY;
		private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
		private int batchSize = DEFAULT_BATCH_SIZE;
		private int threads = DEFAULT_THREAD_COUNT;
		private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

		public Duration getFlushInterval() {
			return flushInterval;
		}

		public void setFlushInterval(Duration interval) {
			this.flushInterval = interval;
		}

		public int getCacheCapacity() {
			return cacheCapacity;
		}

		public void setCacheCapacity(int capacity) {
			this.cacheCapacity = capacity;
		}

		public int getQueueCapacity() {
			return queueCapacity;
		}

		public void setQueueCapacity(int queueCapacity) {
			this.queueCapacity = queueCapacity;
		}

		public int getBatchSize() {
			return batchSize;
		}

		public void setBatchSize(int batchSize) {
			this.batchSize = batchSize;
		}

		public int getThreads() {
			return threads;
		}

		public void setThreads(int threads) {
			this.threads = threads;
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
