package com.redis.smartcache;

import java.time.Duration;
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

public class ConfigManager<T> implements AutoCloseable {

	private static final Logger log = Logger.getLogger(ConfigManager.class.getName());

	private static final String JSON_ROOT = "$";

	private final StatefulRedisModulesConnection<String, String> connection;
	private final Duration refreshRate;
	private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	private final ObjectMapper mapper = new ObjectMapper();
	private final Map<String, ScheduledFuture<?>> futures = new HashMap<>();
	private final Map<String, T> configs = new HashMap<>();

	public ConfigManager(StatefulRedisModulesConnection<String, String> connection, Duration refreshRate) {
		this.connection = connection;
		this.refreshRate = refreshRate;
	}

	/**
	 * 
	 * @param key
	 * @param config
	 * @throws JsonProcessingRuntimeException
	 */
	public synchronized void register(String key, T config) {
		configs.computeIfAbsent(key, k -> doRegister(k, config));
	}

	private static class JsonProcessingRuntimeException extends RuntimeException {

		private static final long serialVersionUID = 33115281494854828L;

		public JsonProcessingRuntimeException(String message, JsonProcessingException cause) {
			super(message, cause);
		}

		@Override
		public synchronized JsonProcessingException getCause() {
			return (JsonProcessingException) super.getCause();
		}

	}

	private T doRegister(String key, T config) {
		log.log(Level.INFO, "Registering config under {0}", key);
		String json;
		try {
			json = mapper.writerFor(config.getClass()).writeValueAsString(config);
		} catch (JsonProcessingException e) {
			throw new JsonProcessingRuntimeException("Could not serialize config to json", e);
		}
		ObjectReader reader = mapper.readerForUpdating(config);
		String reply = connection.sync().jsonSet(key, JSON_ROOT, json, SetMode.NX);
		if ("nil".equalsIgnoreCase(reply)) {
			try {
				read(key, reader);
			} catch (JsonProcessingException e) {
				throw new JsonProcessingRuntimeException("Could not read config from RedisJSON", e);
			}
		}
		futures.put(key, executor.scheduleAtFixedRate(() -> {
			try {
				read(key, reader);
			} catch (Exception e) {
				log.log(Level.SEVERE, String.format("Could not refresh JSON key %s", key), e);
			}
		}, refreshRate.toMillis(), refreshRate.toMillis(), TimeUnit.MILLISECONDS));
		return config;
	}

	private void read(String key, ObjectReader reader) throws JsonProcessingException {
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
		configs.clear();
	}

}