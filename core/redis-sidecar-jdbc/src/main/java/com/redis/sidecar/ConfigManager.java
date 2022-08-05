package com.redis.sidecar;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.json.SetMode;

import io.lettuce.core.RedisURI;

public class ConfigManager implements AutoCloseable {

	private static final Logger log = Logger.getLogger(ConfigManager.class.getName());

	private final RedisManager redisManager;
	private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	private final ObjectMapper mapper = new ObjectMapper();
	private final Map<String, ScheduledFuture<?>> futures = new HashMap<>();
	private final Map<RedisURI, Map<String, Config>> configs = new HashMap<>();

	public ConfigManager(RedisManager redisManager) {
		this.redisManager = redisManager;
	}

	public synchronized Config getConfig(Config config) throws JsonProcessingException {
		String key = key(config);
		RedisURI uri = config.getRedis().uri();
		if (configs.containsKey(uri)) {
			Map<String, Config> keys = configs.get(uri);
			if (keys.containsKey(key)) {
				return keys.get(key);
			}
		} else {
			configs.put(uri, new HashMap<>());
		}
		configs.get(uri).put(key, config);
		StatefulRedisModulesConnection<String, String> connection = redisManager.connection(config);
		String json = mapper.writerFor(config.getClass()).writeValueAsString(config);
		connection.sync().jsonSet(key, "$", json, SetMode.NX);
		ObjectReader reader = mapper.readerForUpdating(config);
		read(connection, key, reader);
		long refreshRateMillis = config.getRefreshRate() * 1000;
		futures.put(key, executor.scheduleAtFixedRate(() -> {
			try {
				read(connection, key, reader);
			} catch (JsonProcessingException e) {
				log.log(Level.SEVERE, String.format("Could not refresh JSON key %s", key), e);
			}
		}, refreshRateMillis, refreshRateMillis, TimeUnit.MILLISECONDS));
		return config;
	}

	public String key(Config config) {
		return config.getRedis().key("config");
	}

	private void read(StatefulRedisModulesConnection<String, String> connection, String key, ObjectReader reader)
			throws JsonProcessingException {
		String json = connection.sync().jsonGet(key);
		if (json == null || json.isEmpty()) {
			return;
		}
		reader.readValue(json);
	}

	@Override
	public void close() {
		futures.forEach((k, v) -> v.cancel(false));
		futures.clear();
	}

}