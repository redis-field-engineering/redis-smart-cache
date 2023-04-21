package com.redis.smartcache.core;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.smartcache.Driver;
import com.redis.smartcache.core.Config.RuleConfig;
import com.redis.smartcache.core.Config.RulesetConfig;
import com.redis.testcontainers.RedisStackContainer;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs.StreamOffset;

@Testcontainers
class ConfigTests {

	@Container
	private final RedisStackContainer redis = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Test
	void keyBuilder() {
		Config config = new Config();
		KeyBuilder keyBuilder = Driver.keyBuilder(config, Driver.KEYSPACE_CACHE);
		String id = "123";
		Assertions.assertEquals(Config.DEFAULT_NAME + KeyBuilder.DEFAULT_SEPARATOR + Driver.KEYSPACE_CACHE
				+ KeyBuilder.DEFAULT_SEPARATOR + id, keyBuilder.build(id));
	}

	@SuppressWarnings("unchecked")
	@Test
	void updateStreamConfig() throws Exception {
		String key = "updateStreamConfig";
		try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
				StatefulRedisModulesConnection<String, String> connection = client.connect()) {
			RulesetConfig conf = new RulesetConfig();
			JavaPropsMapper mapper = Driver.propsMapper();
			try (StreamConfigManager<RulesetConfig> manager = new StreamConfigManager<>(client, key, conf, mapper)) {
				manager.start();
				Assertions.assertNotNull(connection.sync().xread(StreamOffset.latest(key)));
				await().until(manager::isRunning);
				Map<String, String> body = new HashMap<>();
				body.put("rules[0].ttl", "123s");
				connection.sync().xadd(key, body);
				await().until(() -> conf.getRules().size() == 1);
				await().until(() -> conf.getRules().get(0).getTtl().getValue(TimeUnit.SECONDS) == 123);
				body.put("rules[0].ttl", "456s");
				connection.sync().xadd(key, body);
				await().until(() -> conf.getRules().size() == 1);
				await().until(() -> conf.getRules().get(0).getTtl().getValue(TimeUnit.SECONDS) == 456);
			}
			RulesetConfig conf2 = new RulesetConfig();
			try (StreamConfigManager<RulesetConfig> manager2 = new StreamConfigManager<>(client, key, conf2, mapper)) {
				manager2.start();
				await().until(() -> conf2.getRules().size() == 1);
				await().until(() -> conf2.getRules().get(0).getTtl().getValue(TimeUnit.SECONDS) == 456);
			}
		}
	}

	@Test
	void initialStreamConfig() throws Exception {
		String key = "config";
		RulesetConfig conf = new RulesetConfig();
		conf.getRules().add(RuleConfig.tables("customer").ttl(io.airlift.units.Duration.valueOf("1h")).build());
		conf.getRules().add(
				RuleConfig.regex("SELECT \\* FROM customers").ttl(io.airlift.units.Duration.valueOf("30m")).build());
		conf.getRules().add(RuleConfig.passthrough().ttl(io.airlift.units.Duration.valueOf("10s")).build());
		JavaPropsMapper mapper = Driver.propsMapper();
		try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
				StatefulRedisModulesConnection<String, String> connection = client.connect();
				StreamConfigManager<RulesetConfig> manager = new StreamConfigManager<>(client, key, conf, mapper)) {
			manager.start();
			List<StreamMessage<String, String>> messages = connection.sync().xrange(key, Range.unbounded());
			Assertions.assertEquals(1, messages.size());
			Map<String, String> body = messages.get(0).getBody();
			Map<String, String> expectedBody = new HashMap<>();
			expectedBody.put("rules.1.tables.1", "customer");
			expectedBody.put("rules.1.ttl", "1.00h");
			expectedBody.put("rules.2.regex", "SELECT \\* FROM customers");
			expectedBody.put("rules.2.ttl", "30.00m");
			expectedBody.put("rules.3.ttl", "10.00s");
			Assertions.assertEquals(expectedBody, body);
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void disableCaching() throws Exception {
		String key = "disableCaching";
		try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
				StatefulRedisModulesConnection<String, String> connection = client.connect()) {
			RulesetConfig conf = new RulesetConfig();
			conf.getRules().add(RuleConfig.tables("customer").ttl(io.airlift.units.Duration.valueOf("1h")).build());
			conf.getRules().add(RuleConfig.regex("SELECT \\* FROM customers")
					.ttl(io.airlift.units.Duration.valueOf("30m")).build());
			conf.getRules().add(RuleConfig.passthrough().ttl(io.airlift.units.Duration.valueOf("10s")).build());
			JavaPropsMapper mapper = Driver.propsMapper();
			try (StreamConfigManager<RulesetConfig> manager = new StreamConfigManager<>(client, key, conf, mapper)) {
				manager.start();
				Assertions.assertNotNull(connection.sync().xread(StreamOffset.latest(key)));
				await().until(manager::isRunning);
				Map<String, String> body = new HashMap<>();
				body.put("rules[0].ttl", "0s");
				connection.sync().xadd(key, body);
				await().until(() -> conf.getRules().size() == 1);
				await().until(() -> conf.getRules().get(0).getTtl().getValue(TimeUnit.SECONDS) == 0);
			}
		}
	}

	private ConditionFactory await() {
		return Awaitility.await().timeout(Duration.ofSeconds(1));
	}

	@Test
	void configProperties() throws IOException {
		String propertyName = Driver.PROPERTY_PREFIX_REDIS + ".codec-buffer-capacity";
		Config config = new Config();
		DataSize bufferSize = DataSize.of(123, Unit.KILOBYTE);
		config.getRedis().setCodecBufferCapacity(bufferSize);
		Properties properties = Driver.properties(config);
		Assertions.assertEquals(bufferSize, DataSize.valueOf(properties.getProperty(propertyName)));
		Config actual = Driver.config(properties);
		Assertions.assertEquals(config, actual);
		properties.setProperty(propertyName, "10MB");
		Assertions.assertEquals(DataSize.of(10, Unit.MEGABYTE),
				Driver.config(properties).getRedis().getCodecBufferCapacity());
	}

	@Test
	void ruleConfigChange() throws Exception {
		RulesetConfig config = new RulesetConfig();
		EventList eventList = new EventList();
		config.addPropertyChangeListener(eventList);
		RuleConfig newRule = RuleConfig.tables("table1").build();
		config.setRules(Arrays.asList(newRule));
		Assertions.assertEquals(1, eventList.getEvents().size());
		Assertions.assertEquals(RulesetConfig.PROPERTY_RULES, eventList.getEvents().get(0).getPropertyName());
	}

	class EventList implements PropertyChangeListener {

		private final List<PropertyChangeEvent> events = new ArrayList<>();

		@Override
		public void propertyChange(PropertyChangeEvent evt) {
			events.add(evt);
		}

		public List<PropertyChangeEvent> getEvents() {
			return events;
		}
	}

}
