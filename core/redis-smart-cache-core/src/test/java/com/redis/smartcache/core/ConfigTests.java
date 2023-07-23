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
import io.lettuce.core.ScoredValue;
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
        try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
                StatefulRedisModulesConnection<String, String> connection = client.connect()) {
            JavaPropsMapper mapper = Mappers.propsMapper();
            Config config = new Config();
            try (StreamConfigManager manager = new StreamConfigManager(client, config, mapper)) {
                manager.start();
                String key = manager.key();
                Assertions.assertNotNull(connection.sync().xread(StreamOffset.latest(key)));
                await().until(manager::isRunning);
                Map<String, String> body = new HashMap<>();
                body.put("rules[0].ttl", "123s");
                connection.sync().xadd(key, body);
                await().until(() -> config.getRuleset().getRules().length == 1);
                await().until(() -> config.getRuleset().getRules()[0].getTtl().getValue(TimeUnit.SECONDS) == 123);
                body.put("rules[0].ttl", "456s");
                connection.sync().xadd(key, body);
                await().until(() -> config.getRuleset().getRules().length == 1);
                await().until(() -> config.getRuleset().getRules()[0].getTtl().getValue(TimeUnit.SECONDS) == 456);
            }
            Config config2 = new Config();
            try (StreamConfigManager manager2 = new StreamConfigManager(client, config2, mapper)) {
                manager2.start();
                await().until(() -> config2.getRuleset().getRules().length == 1);
                await().until(() -> config2.getRuleset().getRules()[0].getTtl().getValue(TimeUnit.SECONDS) == 456);
            }
        }
    }

    @Test
    void duplicateConfig() throws Exception {
        String key = "smartcache:config";
        try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
                StatefulRedisModulesConnection<String, String> connection = client.connect()) {
            Config config = new Config();
            Map<String, String> body = new HashMap<>();
            body.put("rules[0].ttl", "0s");
            connection.sync().xadd(key, body);
            config.getRuleset().setRules(RuleConfig.passthrough().build());
            JavaPropsMapper mapper = Mappers.propsMapper();
            try (StreamConfigManager manager = new StreamConfigManager(client, config, mapper)) {
                manager.start();
                await().until(manager::isRunning);
                Assertions.assertEquals(1, config.getRuleset().getRules().length);
                Assertions.assertEquals(0, config.getRuleset().getRules()[0].getTtl().getValue(TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void initialStreamConfig() throws Exception {
        JavaPropsMapper mapper = Mappers.propsMapper();
        Config config = new Config();
        RuleConfig rule1 = RuleConfig.tables("customer").ttl(Duration.valueOf("1h")).build();
        RuleConfig rule2 = RuleConfig.regex("SELECT \\* FROM customers").ttl(Duration.valueOf("30m")).build();
        RuleConfig rule3 = RuleConfig.passthrough().ttl(Duration.valueOf("10s")).build();
        RuleConfig rule4 = RuleConfig.queryIds("ab324499").ttl(Duration.valueOf("10s")).build();
        config.getRuleset().setRules(rule1, rule2, rule3, rule4);
        try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
                StatefulRedisModulesConnection<String, String> connection = client.connect();
                StreamConfigManager manager = new StreamConfigManager(client, config, mapper)) {
            manager.start();
            String key = manager.key();
            List<StreamMessage<String, String>> messages = connection.sync().xrange(key, Range.unbounded());
            Assertions.assertEquals(1, messages.size());
            Map<String, String> properties = new HashMap<>();
            properties.put("rules.1.tables.1", "customer");
            properties.put("rules.1.ttl", "1.00h");
            properties.put("rules.2.regex", "SELECT \\* FROM customers");
            properties.put("rules.2.ttl", "30.00m");
            properties.put("rules.3.ttl", "10.00s");
            properties.put("rules.4.query-ids.1", "ab324499");
            properties.put("rules.4.ttl", "10.00s");
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
        try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
                StatefulRedisModulesConnection<String, String> connection = client.connect()) {
            Config config = new Config();
            RuleConfig rule1 = RuleConfig.tables("customer").ttl(Duration.valueOf("1h")).build();
            RuleConfig rule2 = RuleConfig.regex("SELECT \\* FROM customers").ttl(Duration.valueOf("30m")).build();
            RuleConfig rule3 = RuleConfig.passthrough().ttl(Duration.valueOf("10s")).build();
            config.getRuleset().setRules(rule1, rule2, rule3);
            JavaPropsMapper mapper = Mappers.propsMapper();
            try (StreamConfigManager manager = new StreamConfigManager(client, config, mapper)) {
                manager.start();
                String key = manager.key();
                Assertions.assertNotNull(connection.sync().xread(StreamOffset.latest(key)));
                await().until(manager::isRunning);
                Map<String, String> body = new HashMap<>();
                body.put("rules[0].ttl", "0s");
                connection.sync().xadd(key, body);
                await().until(() -> config.getRuleset().getRules().length == 1);
                await().until(() -> config.getRuleset().getRules()[0].getTtl().getValue(TimeUnit.SECONDS) == 0);
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
        Assertions.assertEquals(RulesetConfig.of(rule1.clone(), rule2.clone()), RulesetConfig.of(rule1.clone(), rule2.clone()));
    }

    @SuppressWarnings("unchecked")
    @Test
    void ruleSessionUpdate() throws Exception {
        try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
                StatefulRedisModulesConnection<String, String> connection = client.connect()) {
            Config config = new Config();
            QueryRuleSession session = QueryRuleSession.of(config.getRuleset());
            config.getRuleset().addPropertyChangeListener(session);
            JavaPropsMapper mapper = Mappers.propsMapper();
            try (StreamConfigManager manager = new StreamConfigManager(client, config, mapper)) {
                manager.start();
                String key = manager.key();
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

    @Test
    void configUpdateAck() throws Exception {
        Config config = new Config();
        assertConfigUpdateAck(config);
    }

    @Test
    void configUpdateAckWithID() throws Exception {
        String id = "myAppId";
        Config config = new Config();
        config.setId(id);
        assertConfigUpdateAck(config);
    }

    @SuppressWarnings("unchecked")
    private void assertConfigUpdateAck(Config config) throws Exception {
        try (RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
                StatefulRedisModulesConnection<String, String> connection = client.connect()) {
            JavaPropsMapper mapper = Mappers.propsMapper();
            try (StreamConfigManager manager = new StreamConfigManager(client, config, mapper)) {
                manager.start();
                Assertions.assertNotNull(connection.sync().xread(StreamOffset.latest(manager.key())));
                String id = config.getId() == null ? manager.clientId() : config.getId();
                await().until(manager::isRunning);
                Map<String, String> body = new HashMap<>();
                body.put("rules[0].ttl", "123s");
                assertMessageIdEquals(connection, manager.ackKey(), connection.sync().xadd(manager.key(), body), id);
                body.put("rules[0].ttl", "456s");
                assertMessageIdEquals(connection, manager.ackKey(), connection.sync().xadd(manager.key(), body), id);
            }
        }
    }

    private void assertMessageIdEquals(StatefulRedisModulesConnection<String, String> connection, String ackKey,
            String messageId, String appInstanceId) {
        Awaitility.await().until(() -> !connection.sync().zrangeWithScores(ackKey, 0, -1).isEmpty());
        List<ScoredValue<String>> scoredValues = connection.sync().zrangeWithScores(ackKey, 0, -1);
        Assertions.assertEquals(1, scoredValues.size());
        double score = scoredValues.get(0).getScore();
        Assertions.assertEquals(messageId, (long) score + "-0");
        Assertions.assertEquals(appInstanceId, scoredValues.get(0).getValue());

    }

}
