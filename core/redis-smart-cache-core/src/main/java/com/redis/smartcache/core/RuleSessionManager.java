package com.redis.smartcache.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.redis.smartcache.core.config.Config;
import com.redis.smartcache.core.config.RulesetConfig;

import io.lettuce.core.AbstractRedisClient;

public class RuleSessionManager implements AutoCloseable {

	public static final String KEY_CONFIG = "config";

	private final JavaPropsMapper mapper = Mappers.propsMapper();
	private final Map<Config, ConfigManager<RulesetConfig>> configManagers = new HashMap<>();
	private final Map<Config, QueryRuleSession> ruleSessions = new HashMap<>();
	private final ClientManager clientManager;

	public RuleSessionManager(ClientManager clientManager) {
		this.clientManager = clientManager;
	}

	public QueryRuleSession getRuleSession(Config config) {
		return ruleSessions.computeIfAbsent(config, this::createRuleSession);
	}

	private ConfigManager<RulesetConfig> createConfigManager(Config config) {
		AbstractRedisClient client = clientManager.getClient(config.getRedis());
		String key = KeyBuilder.of(config).build(KEY_CONFIG);
		RulesetConfig ruleset = config.getRuleset();
		StreamConfigManager<RulesetConfig> configManager = new StreamConfigManager<>(client, key, ruleset, mapper);
		try {
			configManager.start();
		} catch (IOException e) {
			throw new IllegalStateException("Could not start config manager", e);
		}
		return configManager;
	}

	@Override
	public void close() throws Exception {
		for (ConfigManager<RulesetConfig> configManager : configManagers.values()) {
			configManager.close();
		}
		configManagers.clear();
	}

	private QueryRuleSession createRuleSession(Config config) {
		ConfigManager<RulesetConfig> configManager = configManagers.computeIfAbsent(config, this::createConfigManager);
		RulesetConfig ruleset = configManager.get();
		QueryRuleSession session = QueryRuleSession.of(ruleset);
		ruleset.addPropertyChangeListener(session);
		return session;
	}

}
