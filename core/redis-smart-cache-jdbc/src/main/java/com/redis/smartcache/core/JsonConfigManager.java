package com.redis.smartcache.core;

import java.time.Duration;
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
import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;

public class JsonConfigManager<T> implements ConfigManager<T> {

	private static final Logger log = Logger.getLogger(JsonConfigManager.class.getName());

	private static final String JSON_ROOT = "$";

	private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	private final AbstractRedisClient client;
	private final String key;
	private final T config;
	private final ObjectMapper mapper;
	private final Duration refreshInterval;
	private ObjectReader reader;
	private StatefulRedisModulesConnection<String, String> connection;
	private ScheduledFuture<?> future;

	public JsonConfigManager(AbstractRedisClient client, String key, T config, ObjectMapper mapper,
			Duration refreshInterval) {
		this.client = client;
		this.key = key;
		this.config = config;
		this.mapper = mapper;
		this.refreshInterval = refreshInterval;
	}

	public void start() throws JsonProcessingException {
		this.connection = RedisModulesUtils.connection(client);
		String json = mapper.writeValueAsString(config);
		String reply = connection.sync().jsonSet(key, JSON_ROOT, json, SetMode.NX);
		if ("nil".equalsIgnoreCase(reply)) { // Key already exists
			refresh();
		}
		this.reader = mapper.readerForUpdating(config);
		this.future = executor.scheduleAtFixedRate(this::refresh, 0, refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
	}

	private void refresh() {
		synchronized (connection) {
			if (!connection.isOpen()) {
				return;
			}
			String json = connection.sync().jsonGet(key);
			if (json == null || json.isEmpty()) {
				return;
			}
			try {
				reader.readValue(json);
			} catch (JsonProcessingException e) {
				log.log(Level.SEVERE, "Could not read JSON", e);
			}
		}
	}

	@Override
	public void stop() {
		future.cancel(false);
		synchronized (connection) {
			connection.close();
		}
	}

	@Override
	public T get() {
		return config;
	}

}