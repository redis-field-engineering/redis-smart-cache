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

public class ConfigManager<T> implements AutoCloseable, Runnable {

	private static final Logger log = Logger.getLogger(ConfigManager.class.getName());

	private static final String JSON_ROOT = "$";

	private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	private final ObjectMapper mapper = new ObjectMapper();
	private final StatefulRedisModulesConnection<String, String> connection;
	private final String key;
	private final T config;
	private final ObjectReader reader;
	private final ScheduledFuture<?> future;

	public ConfigManager(StatefulRedisModulesConnection<String, String> connection, String key, T config,
			Duration period) throws JsonProcessingException {
		this.connection = connection;
		this.key = key;
		this.config = config;
		log.log(Level.INFO, "Registering config under {0}", key);
		String json = mapper.writerFor(config.getClass()).writeValueAsString(config);
		this.reader = mapper.readerForUpdating(config);
		String reply = connection.sync().jsonSet(key, JSON_ROOT, json, SetMode.NX);
		if ("nil".equalsIgnoreCase(reply)) {
			read();
		}
		future = executor.scheduleAtFixedRate(this, period.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void run() {
		try {
			read();
		} catch (Exception e) {
			log.log(Level.SEVERE, String.format("Could not refresh JSON key %s", key), e);
		}
	}

	public T getConfig() {
		return config;
	}

	public T read() throws JsonProcessingException {
		String json = connection.sync().jsonGet(key);
		if (json == null || json.isEmpty()) {
			return null;
		}
		return reader.readValue(json);
	}

	@Override
	public void close() {
		future.cancel(false);
	}

}