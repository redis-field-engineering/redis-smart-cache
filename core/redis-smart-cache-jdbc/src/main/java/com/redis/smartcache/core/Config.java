package com.redis.smartcache.core;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

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

		public static final Duration DEFAULT_STEP = new Duration(60, TimeUnit.SECONDS);

		private boolean enabled = true;
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
		public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
		public static final int DEFAULT_BATCH_SIZE = 50;
		public static final int DEFAULT_CACHE_CAPACITY = 10000;

		private int cacheCapacity = DEFAULT_CACHE_CAPACITY;
		private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
		private int poolSize = DEFAULT_POOL_SIZE;
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

		public int getPoolSize() {
			return poolSize;
		}

		public void setPoolSize(int poolSize) {
			this.poolSize = poolSize;
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

	public static class RulesetConfig {

		public static final String PROPERTY_RULES = "rules";
		public static final Duration DEFAULT_REFRESH_STEP = new Duration(10, TimeUnit.SECONDS);

		private Duration refresh = DEFAULT_REFRESH_STEP;
		private final PropertyChangeSupport support = new PropertyChangeSupport(this);
		private List<RuleConfig> rules;

		/**
		 * 
		 * @return config refresh step duration in seconds
		 */
		public Duration getRefresh() {
			return refresh;
		}

		public void setRefresh(Duration seconds) {
			this.refresh = seconds;
		}

		public RulesetConfig() {
			this(RuleConfig.passthrough().build());
		}

		public RulesetConfig(RuleConfig... rules) {
			this.rules = Arrays.asList(rules);
		}

		public void addPropertyChangeListener(PropertyChangeListener listener) {
			support.addPropertyChangeListener(listener);
			rules.forEach(r -> r.addPropertyChangeListener(listener));
		}

		public void removePropertyChangeListener(PropertyChangeListener listener) {
			support.removePropertyChangeListener(listener);
			rules.forEach(r -> r.removePropertyChangeListener(listener));
		}

		public List<RuleConfig> getRules() {
			return rules;
		}

		public void setRules(List<RuleConfig> rules) {
			support.firePropertyChange(PROPERTY_RULES, this.rules, rules);
			this.rules = rules;
		}

		public static RulesetConfig of(RuleConfig... rules) {
			return new RulesetConfig(rules);
		}

	}

	public static class RuleConfig {

		public static final Duration DEFAULT_TTL = new Duration(1, TimeUnit.HOURS);

		private final PropertyChangeSupport support = new PropertyChangeSupport(this);

		private List<String> tables;
		private List<String> tablesAny;
		private List<String> tablesAll;
		private String regex;
		private List<String> queryIds;
		private Duration ttl = DEFAULT_TTL;

		public RuleConfig() {
		}

		private RuleConfig(Builder builder) {
			this.tables = builder.tables;
			this.tablesAny = builder.tablesAny;
			this.tablesAll = builder.tablesAll;
			this.regex = builder.regex;
			this.queryIds = builder.queryIds;
			this.ttl = builder.ttl;
		}

		public void addPropertyChangeListener(PropertyChangeListener listener) {
			support.addPropertyChangeListener(listener);
		}

		public void removePropertyChangeListener(PropertyChangeListener listener) {
			support.removePropertyChangeListener(listener);
		}

		public List<String> getQueryIds() {
			return queryIds;
		}

		public void setQueryIds(List<String> queryIds) {
			support.firePropertyChange("queryIds", this.queryIds, queryIds);
			this.queryIds = queryIds;
		}

		public String getRegex() {
			return regex;
		}

		public void setRegex(String regex) {
			support.firePropertyChange("regex", this.regex, regex);
			this.regex = regex;
		}

		public List<String> getTables() {
			return tables;
		}

		public void setTables(List<String> tables) {
			support.firePropertyChange("tables", this.tables, tables);
			this.tables = tables;
		}

		public List<String> getTablesAny() {
			return tablesAny;
		}

		public void setTablesAny(List<String> tablesAny) {
			support.firePropertyChange("tablesAny", this.tablesAny, tablesAny);
			this.tablesAny = tablesAny;
		}

		public List<String> getTablesAll() {
			return tablesAll;
		}

		public void setTablesAll(List<String> tablesAll) {
			support.firePropertyChange("tablesAll", this.tablesAll, tablesAll);
			this.tablesAll = tablesAll;
		}

		/**
		 * 
		 * @return Key expiration duration. Use a duration of zero for no caching
		 */
		public Duration getTtl() {
			return ttl;
		}

		public void setTtl(Duration ttl) {
			support.firePropertyChange("ttl", this.ttl, ttl);
			this.ttl = ttl;
		}

		@Override
		public String toString() {
			return "RuleConfig [tables=" + tables + ", tablesAny=" + tablesAny + ", tablesAll=" + tablesAll + ", regex="
					+ regex + ", queryIds=" + queryIds + ", ttl=" + ttl + "]";
		}

		public static Builder tables(String... tables) {
			Builder builder = new Builder();
			builder.tables = Arrays.asList(tables);
			return builder;
		}

		public static Builder tablesAny(String... tables) {
			Builder builder = new Builder();
			builder.tablesAny = Arrays.asList(tables);
			return builder;
		}

		public static Builder tablesAll(String... tables) {
			Builder builder = new Builder();
			builder.tablesAll = Arrays.asList(tables);
			return builder;
		}

		public static Builder queryIds(String... ids) {
			Builder builder = new Builder();
			builder.queryIds = Arrays.asList(ids);
			return builder;
		}

		public static Builder regex(String regex) {
			Builder builder = new Builder();
			builder.regex = regex;
			return builder;
		}

		public static Builder passthrough() {
			return new Builder();
		}

		public static final class Builder {

			private List<String> tables;
			private List<String> tablesAny;
			private List<String> tablesAll;
			private String regex;
			private List<String> queryIds;
			private Duration ttl = DEFAULT_TTL;

			private Builder() {
			}

			public Builder tables(String... tables) {
				this.tables = Arrays.asList(tables);
				return this;
			}

			public Builder tablesAny(String... tables) {
				this.tablesAny = Arrays.asList(tables);
				return this;
			}

			public Builder tablesAll(String... tables) {
				this.tablesAll = Arrays.asList(tables);
				return this;
			}

			public Builder regex(String regex) {
				this.regex = regex;
				return this;
			}

			public Builder queryIds(String... ids) {
				this.queryIds = Arrays.asList(ids);
				return this;
			}

			public Builder ttl(Duration ttl) {
				this.ttl = ttl;
				return this;
			}

			public RuleConfig build() {
				return new RuleConfig(this);
			}
		}

	}

}
