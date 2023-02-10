package com.redis.smartcache;

import java.sql.ResultSet;
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
import com.redis.smartcache.BootstrapConfig.RedisConfig;
import com.redis.smartcache.codec.ResultSetCodec;
import com.redis.smartcache.rowset.CachedRowSetFactory;

import io.airlift.units.DataSize;
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
	private ConfigManager<RulesetConfig> configManager;
	private StatementRuleSession ruleSession;
	private GenericObjectPool<StatefulRedisModulesConnection<String, ResultSet>> connectionPool;
	private MeterRegistry meterRegistry;
	private ResultSetCache cache;
	private RowSetFactory rowSetFactory;

	public ConnectionContext() {
		this(new BootstrapConfig());
	}

	public ConnectionContext(BootstrapConfig bootstrapConfig) {
		this.bootstrapConfig = bootstrapConfig;
	}

	public BootstrapConfig getBootstrapConfig() {
		return bootstrapConfig;
	}

	public ConfigManager<RulesetConfig> getConfigManager() {
		synchronized (bootstrapConfig) {
			if (configManager == null) {
				StatefulRedisModulesConnection<String, String> connection = connectionSupplier(StringCodec.UTF8).get();
				configManager = new ConfigManager<>(connection, bootstrapConfig.getConfigStep());
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
				DataSize bufferSize = bootstrapConfig.getRedis().getCodecBufferSize();
				ResultSetCodec codec = ResultSetCodec.builder()
						.maxByteBufferCapacity(Math.toIntExact(bufferSize.toBytes())).build();
				GenericObjectPoolConfig<StatefulRedisModulesConnection<String, ResultSet>> poolConfig = new GenericObjectPoolConfig<>();
				poolConfig.setMaxTotal(bootstrapConfig.getRedis().getPoolSize());
				connectionPool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(codec), poolConfig);
			}
		}
		return connectionPool;
	}

	private <K, V> Supplier<StatefulRedisModulesConnection<K, V>> connectionSupplier(RedisCodec<K, V> codec) {
		AbstractRedisClient client = getRedisClient();
		if (client instanceof RedisModulesClusterClient) {
			return () -> ((RedisModulesClusterClient) client).connect(codec);
		}
		return () -> ((RedisModulesClient) client).connect(codec);
	}

	public StatementRuleSession getRuleSession() {
		synchronized (bootstrapConfig) {
			if (ruleSession == null) {
				ruleSession = new StatementRuleSession();
				RulesetConfig rulesConfig = new RulesetConfig();
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
				// TODO introduce a QueryExecutor like PgConnection does
				cache = new ResultSetCacheImpl(getConnectionPool(), getMeterRegistry());
			}
		}
		return cache;

	}

	public RowSetFactory getRowSetFactory() {
		synchronized (bootstrapConfig) {
			if (rowSetFactory == null) {
				rowSetFactory = new CachedRowSetFactory();
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
