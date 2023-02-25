package com.redis.smartcache.core;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.lettuce.core.SslVerifyMode;

public class Config {

	public static final String DEFAULT_KEYSPACE = "smartcache";
	public static final String DEFAULT_KEY_SEPARATOR = ":";
	public static final Duration DEFAULT_CONFIG_STEP = new Duration(10, TimeUnit.SECONDS);
	public static final Duration DEFAULT_METRICS_STEP = new Duration(60, TimeUnit.SECONDS);
	public static final int DEFAULT_POOL_SIZE = 8;
	public static final DataSize DEFAULT_BYTE_BUFFER_CAPACITY = DataSize.of(10, Unit.MEGABYTE);
	private static final int DEFAULT_QUERY_CACHE_CAPACITY = 10000;

	private String keyspace = DEFAULT_KEYSPACE;
	private String keySeparator = DEFAULT_KEY_SEPARATOR;
	private DataSize codecBufferSize = DEFAULT_BYTE_BUFFER_CAPACITY;
	private Duration configStep = DEFAULT_CONFIG_STEP;
	private Duration metricsStep = DEFAULT_METRICS_STEP;
	private int queryCacheCapacity = DEFAULT_QUERY_CACHE_CAPACITY;
	private DriverConfig driver = new DriverConfig();
	private RulesetConfig ruleset = new RulesetConfig();
	private RedisConfig redis = new RedisConfig();

	public String key(String... ids) {
		StringBuilder builder = new StringBuilder(keyspace);
		for (String id : ids) {
			builder.append(keySeparator).append(id);
		}
		return builder.toString();
	}

	public int getQueryCacheCapacity() {
		return queryCacheCapacity;
	}

	public void setQueryCacheCapacity(int queryCacheCapacity) {
		this.queryCacheCapacity = queryCacheCapacity;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public String getKeySeparator() {
		return keySeparator;
	}

	public void setKeySeparator(String keySeparator) {
		this.keySeparator = keySeparator;
	}

	public RedisConfig getRedis() {
		return redis;
	}

	public void setRedis(RedisConfig redis) {
		this.redis = redis;
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

	public RulesetConfig getRuleset() {
		return ruleset;
	}

	public void setRuleset(RulesetConfig ruleset) {
		this.ruleset = ruleset;
	}

	public static class RedisConfig {

		private String uri;
		private boolean cluster;
		private String username;
		private char[] password;
		private TLS tls = new TLS();

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

		public TLS getTls() {
			return tls;
		}

		public void setTls(TLS tls) {
			this.tls = tls;
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

	@Override
	public int hashCode() {
		return Objects.hash(redis.getUri(), keyspace);
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
		return Objects.equals(redis.getUri(), other.redis.getUri()) && Objects.equals(keyspace, other.keyspace);
	}

	public static class TLS {

		private boolean enabled;
		private SslVerifyMode verify = SslVerifyMode.NONE;

		public boolean isEnabled() {
			return enabled;
		}

		public void setEnabled(boolean tls) {
			this.enabled = tls;
		}

		public SslVerifyMode getVerify() {
			return verify;
		}

		public void setVerify(SslVerifyMode mode) {
			this.verify = mode;
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

		private List<RuleConfig> rules;

		public RulesetConfig() {
			this(RuleConfig.passthrough().build());
		}

		public RulesetConfig(RuleConfig... rules) {
			this.rules = Arrays.asList(rules);
		}

		private final PropertyChangeSupport support = new PropertyChangeSupport(this);

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

		public static class RuleConfig {

			public static final Duration DEFAULT_TTL = new Duration(1, TimeUnit.HOURS);

			private final PropertyChangeSupport support = new PropertyChangeSupport(this);

			private String[] tables;
			private String[] tablesAny;
			private String[] tablesAll;
			private String regex;
			private Duration ttl = DEFAULT_TTL;

			public RuleConfig() {
			}

			private RuleConfig(Builder builder) {
				this.tables = builder.tables;
				this.tablesAny = builder.tablesAny;
				this.tablesAll = builder.tablesAll;
				this.regex = builder.regex;
				this.ttl = builder.ttl;
			}

			public void addPropertyChangeListener(PropertyChangeListener listener) {
				support.addPropertyChangeListener(listener);
			}

			public void removePropertyChangeListener(PropertyChangeListener listener) {
				support.removePropertyChangeListener(listener);
			}

			public String getRegex() {
				return regex;
			}

			public void setRegex(String regex) {
				support.firePropertyChange("regex", this.regex, regex);
				this.regex = regex;
			}

			public String[] getTables() {
				return tables;
			}

			public void setTables(String... tables) {
				support.firePropertyChange("tables", this.tables, tables);
				this.tables = tables;
			}

			public String[] getTablesAny() {
				return tablesAny;
			}

			public void setTablesAny(String... tablesAny) {
				support.firePropertyChange("tablesAny", this.tablesAny, tablesAny);
				this.tablesAny = tablesAny;
			}

			public String[] getTablesAll() {
				return tablesAll;
			}

			public void setTablesAll(String... tablesAll) {
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
				return "RuleConfig [tables=" + tables + ", tablesAny=" + tablesAny + ", tablesAll=" + tablesAll
						+ ", regex=" + regex + ", ttl=" + ttl + "]";
			}

			public static Builder tables(String... tables) {
				Builder builder = new Builder();
				builder.tables = tables;
				return builder;
			}

			public static Builder tablesAny(String... tables) {
				Builder builder = new Builder();
				builder.tablesAny = tables;
				return builder;
			}

			public static Builder tablesAll(String... tables) {
				Builder builder = new Builder();
				builder.tablesAll = tables;
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

				private String[] tables;
				private String[] tablesAny;
				private String[] tablesAll;
				private String regex;
				private Duration ttl = DEFAULT_TTL;

				private Builder() {
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

}
