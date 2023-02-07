package com.redis.sidecar;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.function.Supplier;

import javax.sql.rowset.RowSetFactory;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisURIBuilder;
import com.redis.micrometer.RedisTimeSeriesMeterRegistry;
import com.redis.sidecar.BootstrapConfig.PoolConfig;
import com.redis.sidecar.BootstrapConfig.RedisConfig;
import com.redis.sidecar.codec.ResultSetCodec;
import com.redis.sidecar.rowset.SidecarRowSetFactory;
import com.redis.sidecar.rules.RuleSession;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;

public class ConnectionContext {

	private final BootstrapConfig bootstrapConfig;
	private AbstractRedisClient redisClient;
	private ConfigManager<RulesConfig> configManager;
	private RuleSession ruleSession;
	private GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>> connectionPool;
	private MeterRegistry meterRegistry;
	private ResultSetCache cache;
	private RowSetFactory rowSetFactory;

	public ConnectionContext(BootstrapConfig bootstrapConfig) {
		this.bootstrapConfig = bootstrapConfig;
	}

	public BootstrapConfig getBootstrapConfig() {
		return bootstrapConfig;
	}

	public ConfigManager<RulesConfig> getConfigManager() {
		synchronized (bootstrapConfig) {
			if (configManager == null) {
				StatefulRedisModulesConnection<String, String> connection = connectionSupplier(StringCodec.UTF8).get();
				Duration refreshRate = Duration.ofSeconds(bootstrapConfig.getConfigStep());
				configManager = new ConfigManager<>(connection, refreshRate);
			}
		}
		return configManager;
	}

	public AbstractRedisClient getRedisClient() {
		synchronized (bootstrapConfig) {
			if (redisClient == null) {
				redisClient = client(bootstrapConfig.getRedis());
			}
		}
		return redisClient;
	}

	private AbstractRedisClient client(RedisConfig config) {
		RedisURIBuilder builder = RedisURIBuilder.create();
		builder.uriString(config.getUri());
		builder.username(config.getUsername());
		builder.password(config.getPassword());
		builder.sslVerifyMode(config.getTlsVerify());
		builder.ssl(config.isTls());
		RedisURI redisURI = builder.build();
		return ClientBuilder.create(redisURI).cluster(config.isCluster()).build();
	}

	public GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>> getConnectionPool() {
		synchronized (bootstrapConfig) {
			if (connectionPool == null) {
				int bufferSize = bootstrapConfig.getRedis().getCodecBufferSize();
				ResultSetCodec codec = ResultSetCodec.builder().maxByteBufferCapacity(bufferSize).build();
				connectionPool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(codec), poolConfig());
			}
		}
		return connectionPool;
	}

	private <K, V> GenericObjectPoolConfig<StatefulRedisModulesConnection<K, V>> poolConfig() {
		PoolConfig pool = bootstrapConfig.getRedis().getPool();
		GenericObjectPoolConfig<StatefulRedisModulesConnection<K, V>> config = new GenericObjectPoolConfig<>();
		config.setMaxTotal(pool.getMaxActive());
		config.setMaxIdle(pool.getMaxIdle());
		config.setMinIdle(pool.getMinIdle());
		config.setTimeBetweenEvictionRuns(Duration.ofMillis(pool.getTimeBetweenEvictionRuns()));
		config.setMaxWait(Duration.ofMillis(pool.getMaxWait()));
		return config;
	}

	private <K, V> Supplier<StatefulRedisModulesConnection<K, V>> connectionSupplier(RedisCodec<K, V> codec) {
		AbstractRedisClient client = getRedisClient();
		if (client instanceof RedisModulesClusterClient) {
			return () -> ((RedisModulesClusterClient) client).connect(codec);
		}
		return () -> ((RedisModulesClient) client).connect(codec);
	}

	public RuleSession getRuleSession() {
		synchronized (bootstrapConfig) {
			if (ruleSession == null) {
				ruleSession = new RuleSession();
				RulesConfig rulesConfig = new RulesConfig();
				ruleSession.updateRules(rulesConfig.getRules());
				rulesConfig.addPropertyChangeListener(ruleSession);
				getConfigManager().register(bootstrapConfig.key("config"), rulesConfig);
			}
		}
		return ruleSession;
	}

	public MeterRegistry getMeterRegistry() {
		if (meterRegistry == null) {
			MeterRegistryConfig config = new MeterRegistryConfig(bootstrapConfig);
			meterRegistry = new RedisTimeSeriesMeterRegistry(config, Clock.SYSTEM, getRedisClient());
		}
		return meterRegistry;
	}

	public ResultSetCache getCache() {
		synchronized (bootstrapConfig) {
			if (cache == null) {
				cache = new ResultSetCacheImpl(getMeterRegistry(), getConnectionPool(), bootstrapConfig.key("cache"));
			}
		}
		return cache;

	}

	public RowSetFactory getRowSetFactory() {
		synchronized (bootstrapConfig) {
			if (rowSetFactory == null) {
				rowSetFactory = new SidecarRowSetFactory();
			}
		}
		return rowSetFactory;
	}

	public void clear() {
		if (configManager != null) {
			configManager.close();
			configManager = null;
		}
		if (meterRegistry != null) {
			meterRegistry.close();
			meterRegistry = null;
		}
		if (connectionPool != null) {
			connectionPool.close();
			connectionPool = null;
		}
		if (redisClient != null) {
			redisClient.shutdown();
			redisClient.getResources().shutdown();
			redisClient = null;
		}
		cache = null;
		rowSetFactory = null;
		ruleSession = null;
	}

}
