package com.redis.smartcache.core;

import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.smartcache.core.RulesetConfig;
import com.redis.smartcache.core.RuleConfig;

import io.lettuce.core.AbstractRedisClient;

public class RulesetManager implements AutoCloseable {

	public static final String KEY_CONFIG = "config";

	private final JavaPropsMapper mapper = Mappers.propsMapper();
	private final Map<Config, ConfigManager<RulesetConfig>> configManagers = new HashMap<>();
	private final ClientManager clientManager;

	public RulesetManager(ClientManager clientManager) {
		this.clientManager = clientManager;
	}

	public RulesetConfig getRuleset(Config config) {
		return configManagers.computeIfAbsent(config, this::createConfigManager).get();
	}

	private ConfigManager<RulesetConfig> createConfigManager(Config config) {
		AbstractRedisClient client = clientManager.getClient(config);
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

}
