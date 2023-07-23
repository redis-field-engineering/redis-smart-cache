package com.redis.smartcache.core;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.smartcache.core.config.Config;
import com.redis.smartcache.core.config.RulesetConfig;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;

public class StreamConfigManager implements ConfigManager<RulesetConfig>, Consumer<StreamMessage<String, String>> {

    private static final Logger log = Logger.getLogger(StreamConfigManager.class.getName());

    public static final Duration DEFAULT_BLOCK = Duration.ofMillis(300);

    public static final OptionalLong DEFAULT_COUNT = OptionalLong.empty();

    private final AbstractRedisClient client;

    private final JavaPropsMapper mapper;

    private final Config config;

    private Duration block = DEFAULT_BLOCK;

    private OptionalLong count = DEFAULT_COUNT;

    private StreamPoller poller;

    private ExecutorService executor;

    private StatefulRedisModulesConnection<String, String> connection;

    public StreamConfigManager(AbstractRedisClient client, Config config, JavaPropsMapper mapper) {
        this.config = config;
        this.client = client;
        this.mapper = mapper;
    }

    public void setBlock(Duration block) {
        this.block = block;
    }

    public void setCount(long count) {
        setCount(OptionalLong.of(count));
    }

    public void setCount(OptionalLong count) {
        this.count = count;
    }

    @Override
    public void start() throws IOException {
        String key = key();
        connection = RedisModulesUtils.connection(client);
        List<StreamMessage<String, String>> messages = connection.sync().xrevrange(key, Range.create("-", "+"),
                Limit.create(0, 1));
        if (messages.isEmpty()) {
            Map<String, String> map = mapper.writeValueAsMap(config.getRuleset());
            if (!map.isEmpty()) {
                connection.sync().xadd(key, map);
            }
        } else {
            accept(messages.get(0));
        }
        executor = Executors.newSingleThreadExecutor();
        poller = new StreamPoller(connection, xreadArgs(), StreamOffset.latest(key), this);
        executor.submit(poller);
    }

    public String key() {
        return keyBuilder().build();
    }

    private KeyBuilder keyBuilder() {
        return KeyBuilder.of(config).withKeyspace("config");
    }

    @Override
    public void accept(StreamMessage<String, String> message) {
        try {
            RulesetConfig newConfig = mapper.readMapAs(message.getBody(), RulesetConfig.class);
            if (newConfig != null) {
                config.getRuleset().setRules(newConfig.getRules());
                double score = score(message);
                String appInstanceId = appInstanceId();
                String ackKey = ackKey();
                connection.sync().zadd(ackKey, score, appInstanceId);
                log.log(Level.INFO, "Updated configuration id {0}: {1}", new Object[] { message.getId(), config.getRuleset() });
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Could not parse config", e);
        }
    }

    private String appInstanceId() {
        if (config.getId() == null) {
            return clientId();
        }
        return config.getId();
    }

    public String clientId() {
        return String.valueOf(connection.sync().clientId());
    }

    public String ackKey() {
        return keyBuilder().build("ack");
    }

    private double score(StreamMessage<String, String> message) {
        String id = message.getId();
        int offsetPosition = id.indexOf("-");
        if (offsetPosition == -1) {
            return Long.parseLong(id);
        }
        long millis = Long.parseLong(id.substring(0, offsetPosition));
        double sequence = Long.parseLong(id.substring(offsetPosition + 1));
        return millis + sequence / 1000;
    }

    public boolean isRunning() {
        return poller.getState() == State.STARTED;
    }

    private XReadArgs xreadArgs() {
        XReadArgs args = new XReadArgs();
        if (!block.isZero()) {
            args.block(block);
        }
        count.ifPresent(args::count);
        return args;
    }

    @Override
    public RulesetConfig get() {
        return config.getRuleset();
    }

    @Override
    public void stop() throws InterruptedException, ExecutionException, TimeoutException {
        poller.stop();
        poller = null;
        executor.shutdown();
        executor = null;
        connection.close();
        connection = null;
    }

}
