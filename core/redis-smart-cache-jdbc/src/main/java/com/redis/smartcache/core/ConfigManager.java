package com.redis.smartcache.core;

import java.text.MessageFormat;
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

public class ConfigManager<T> implements AutoCloseable {

	private static final Logger log = Logger.getLogger(ConfigManager.class.getName());

	private static final String JSON_ROOT = "$";

	private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	private final String key;
	private final T config;
	private final StatefulRedisModulesConnection<String, String> connection;
	private ObjectReader reader;
	private ScheduledFuture<?> future;

	public ConfigManager(AbstractRedisClient client, ObjectMapper mapper, String key, T config, Duration period)
			throws JsonProcessingException {
		this.key = key;
		this.config = config;
		this.connection = RedisModulesUtils.connection(client);
		log.log(Level.INFO, "Registering config under {0}", key);
		String json = mapper.writerFor(config.getClass()).writeValueAsString(config);
		this.reader = mapper.readerForUpdating(config);
		String reply = connection.sync().jsonSet(key, JSON_ROOT, json, SetMode.NX);
		if ("nil".equalsIgnoreCase(reply)) {
			read();
		}
		future = executor.scheduleAtFixedRate(this::safeRead, 0, period.toMillis(), TimeUnit.MILLISECONDS);
	}

	public T getConfig() {
		return config;
	}

	@Override
	public void close() {
		future.cancel(false);
		connection.close();
	}

	public T read() throws JsonProcessingException {
		String json = connection.sync().jsonGet(key);
		if (json == null || json.isEmpty()) {
			return null;
		}
		return reader.readValue(json);
	}

	private void safeRead() {
		try {
			read();
		} catch (Exception e) {
			log.log(Level.SEVERE, e, () -> MessageFormat.format("Could not read JSON key {0}", key));
		}
	}

}