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

public class ConfigManager implements AutoCloseable {

	private static final Logger log = Logger.getLogger(ConfigManager.class.getName());

	private static final String JSON_ROOT = "$";

	private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	private final ObjectMapper mapper = new ObjectMapper();
	private final Map<String, ScheduledFuture<?>> futures = new HashMap<>();
	private final Map<String, Config> configs = new HashMap<>();

	public synchronized Config getConfig(String key, StatefulRedisModulesConnection<String, String> connection,
			Config config) throws JsonProcessingException {
		if (configs.containsKey(key)) {
			return configs.get(key);
		}
		configs.put(key, config);
		String json = mapper.writerFor(config.getClass()).writeValueAsString(config);
		connection.sync().jsonSet(key, JSON_ROOT, json, SetMode.NX);
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