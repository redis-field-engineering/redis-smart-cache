package com.redis.smartcache.core;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.redis.smartcache.core.config.Config;
import com.redis.smartcache.core.config.RulesetConfig;
import com.redis.testcontainers.RedisStackContainer;

import io.airlift.units.Duration;
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
		String keyspace = "cache";
		KeyBuilder keyBuilder = KeyBuilder.of(config).sub(keyspace);
		String id = "123";
		Assertions.assertEquals(
				Config.DEFAULT_NAME + KeyBuilder.DEFAULT_SEPARATOR + keyspace + KeyBuilder.DEFAULT_SEPARATOR + id,
				keyBuilder.build(id));
	}

	@SuppressWarnings("unchecked")
	@Test
	void updateStreamConfig() throws Exception {
		String key = "updateStreamConfig";
		try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
				StatefulRedisModulesConnection<String, String> connection = client.connect()) {
			RulesetConfig conf = new RulesetConfig();
			JavaPropsMapper mapper = Mappers.propsMapper();
			try (StreamConfigManager<RulesetConfig> manager = new StreamConfigManager<>(client, key, conf, mapper)) {
				manager.start();
				Assertions.assertNotNull(connection.sync().xread(StreamOffset.latest(key)));
				await().until(manager::isRunning);
				Map<String, String> body = new HashMap<>();
				body.put("rules[0].ttl", "123s");
				connection.sync().xadd(key, body);
				await().until(() -> conf.getRules().length == 1);
				await().until(() -> conf.getRules()[0].getTtl().getValue(TimeUnit.SECONDS) == 123);
				body.put("rules[0].ttl", "456s");
				connection.sync().xadd(key, body);
				await().until(() -> conf.getRules().length == 1);
				await().until(() -> conf.getRules()[0].getTtl().getValue(TimeUnit.SECONDS) == 456);
			}
			RulesetConfig conf2 = new RulesetConfig();
			try (StreamConfigManager<RulesetConfig> manager2 = new StreamConfigManager<>(client, key, conf2, mapper)) {
				manager2.start();
				await().until(() -> conf2.getRules().length == 1);
				await().until(() -> conf2.getRules()[0].getTtl().getValue(TimeUnit.SECONDS) == 456);
			}
		}
	}

	@Test
	void duplicateConfig() throws Exception {
		String key = "duplicateStreamConfig";
		try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
				StatefulRedisModulesConnection<String, String> connection = client.connect()) {
			Map<String, String> body = new HashMap<>();
			body.put("rules[0].ttl", "0s");
			connection.sync().xadd(key, body);
			RulesetConfig conf = new RulesetConfig();
			conf.setRules(RuleConfig.passthrough().build());
			JavaPropsMapper mapper = Mappers.propsMapper();
			try (StreamConfigManager<RulesetConfig> manager = new StreamConfigManager<>(client, key, conf, mapper)) {
				manager.start();
				await().until(manager::isRunning);
				Assertions.assertEquals(1, conf.getRules().length);
				Assertions.assertEquals(0, conf.getRules()[0].getTtl().getValue(TimeUnit.SECONDS));
			}
		}
	}

	@Test
	void initialStreamConfig() throws Exception {
		JavaPropsMapper mapper = Mappers.propsMapper();
		Map<String, String> properties = new HashMap<>();
		properties.put("rules.1.tables.1", "customer");
		properties.put("rules.1.ttl", "1.00h");
		properties.put("rules.2.regex", "SELECT \\* FROM customers");
		properties.put("rules.2.ttl", "30.00m");
		properties.put("rules.3.ttl", "10.00s");
		properties.put("rules.4.query-ids.1", "ab324499");
		properties.put("rules.4.ttl", "10.00s");
		RulesetConfig conf = new RulesetConfig();
		RuleConfig rule1 = RuleConfig.tables("customer").ttl(Duration.valueOf("1h")).build();
		RuleConfig rule2 = RuleConfig.regex("SELECT \\* FROM customers").ttl(Duration.valueOf("30m")).build();
		RuleConfig rule3 = RuleConfig.passthrough().ttl(Duration.valueOf("10s")).build();
		RuleConfig rule4 = RuleConfig.queryIds("ab324499").ttl(Duration.valueOf("10s")).build();
		conf.setRules(rule1, rule2, rule3, rule4);
		String key = "config";
		try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
				StatefulRedisModulesConnection<String, String> connection = client.connect();
				StreamConfigManager<RulesetConfig> manager = new StreamConfigManager<>(client, key, conf, mapper)) {
			manager.start();
			List<StreamMessage<String, String>> messages = connection.sync().xrange(key, Range.unbounded());
			Assertions.assertEquals(1, messages.size());
			Assertions.assertEquals(properties, messages.get(0).getBody());
		}
	}

	@Test
	void configMapper() throws IOException {
		JavaPropsMapper mapper = Mappers.propsMapper();
		Map<String, String> properties = new HashMap<>();
		properties.put("rules.1.tables.1", "customer");
		properties.put("rules.1.ttl", "1.00h");
		properties.put("rules.2.regex", "SELECT \\* FROM customers");
		properties.put("rules.2.ttl", "30.00m");
		properties.put("rules.3.ttl", "10.00s");
		properties.put("rules.4.query-ids.1", "ab324499");
		properties.put("rules.4.ttl", "10.00s");
		RulesetConfig conf = new RulesetConfig();
		RuleConfig rule1 = RuleConfig.tables("customer").ttl(Duration.valueOf("1h")).build();
		RuleConfig rule2 = RuleConfig.regex("SELECT \\* FROM customers").ttl(Duration.valueOf("30m")).build();
		RuleConfig rule3 = RuleConfig.passthrough().ttl(Duration.valueOf("10s")).build();
		RuleConfig rule4 = RuleConfig.queryIds("ab324499").ttl(Duration.valueOf("10s")).build();
		conf.setRules(rule1, rule2, rule3, rule4);
		Assertions.assertEquals(conf, mapper.readMapAs(properties, RulesetConfig.class));
		Assertions.assertEquals(properties, mapper.writeValueAsMap(conf));
	}

	@SuppressWarnings("unchecked")
	@Test
	void disableCaching() throws Exception {
		String key = "disableCaching";
		try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
				StatefulRedisModulesConnection<String, String> connection = client.connect()) {
			RulesetConfig conf = new RulesetConfig();
			RuleConfig rule1 = RuleConfig.tables("customer").ttl(Duration.valueOf("1h")).build();
			RuleConfig rule2 = RuleConfig.regex("SELECT \\* FROM customers").ttl(Duration.valueOf("30m")).build();
			RuleConfig rule3 = RuleConfig.passthrough().ttl(Duration.valueOf("10s")).build();
			conf.setRules(rule1, rule2, rule3);
			JavaPropsMapper mapper = Mappers.propsMapper();
			try (StreamConfigManager<RulesetConfig> manager = new StreamConfigManager<>(client, key, conf, mapper)) {
				manager.start();
				Assertions.assertNotNull(connection.sync().xread(StreamOffset.latest(key)));
				await().until(manager::isRunning);
				Map<String, String> body = new HashMap<>();
				body.put("rules[0].ttl", "0s");
				connection.sync().xadd(key, body);
				await().until(() -> conf.getRules().length == 1);
				await().until(() -> conf.getRules()[0].getTtl().getValue(TimeUnit.SECONDS) == 0);
			}
		}
	}

	private ConditionFactory await() {
		return Awaitility.await().timeout(java.time.Duration.ofSeconds(1));
	}

	@Test
	void ruleConfigChange() throws Exception {
		RulesetConfig config = new RulesetConfig();
		EventList eventList = new EventList();
		config.addPropertyChangeListener(eventList);
		RuleConfig newRule = RuleConfig.tables("table1").build();
		config.setRules(newRule);
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

	@Test
	void rulesetEquals() {
		RuleConfig rule1 = RuleConfig.tables("customer").ttl(Duration.valueOf("1h")).build();
		RuleConfig rule2 = RuleConfig.regex("SELECT \\* FROM customers").ttl(Duration.valueOf("30m")).build();
		Assertions.assertNotEquals(RulesetConfig.of(rule1.clone(), rule2.clone()), RulesetConfig.of(rule1.clone()));
		Assertions.assertNotEquals(RulesetConfig.of(rule1.clone(), rule2.clone()),
				RulesetConfig.of(rule2.clone(), rule1.clone()));
		Assertions.assertEquals(RulesetConfig.of(rule1.clone(), rule2.clone()),
				RulesetConfig.of(rule1.clone(), rule2.clone()));
	}

	@SuppressWarnings("unchecked")
	@Test
	void ruleSessionUpdate() throws Exception {
		String key = "updateStreamConfig";
		try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
				StatefulRedisModulesConnection<String, String> connection = client.connect()) {
			RulesetConfig rulesetConfig = new RulesetConfig();
			QueryRuleSession session = QueryRuleSession.of(rulesetConfig);
			rulesetConfig.addPropertyChangeListener(session);
			JavaPropsMapper mapper = Mappers.propsMapper();
			try (StreamConfigManager<RulesetConfig> manager = new StreamConfigManager<>(client, key, rulesetConfig,
					mapper)) {
				manager.start();
				Assertions.assertNotNull(connection.sync().xread(StreamOffset.latest(key)));
				await().until(manager::isRunning);
				Map<String, String> body = new HashMap<>();
				body.put("rules[0].ttl", "123s");
				connection.sync().xadd(key, body);
				await().until(() -> {
					Action action = new Action();
					session.getRules().get(0).getAction().accept(action);
					return action.getTtl() == 123000;
				});
				body.put("rules[0].ttl", "456s");
				connection.sync().xadd(key, body);
				await().until(() -> {
					Action action = new Action();
					session.getRules().get(0).getAction().accept(action);
					return action.getTtl() == 456000;
				});
			}

		}
	}

}
