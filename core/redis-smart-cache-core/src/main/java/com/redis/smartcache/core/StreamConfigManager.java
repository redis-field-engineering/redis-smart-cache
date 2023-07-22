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

import org.springframework.beans.BeanUtils;

import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;

public class StreamConfigManager<T> implements ConfigManager<T>, Consumer<StreamMessage<String, String>> {

    private static final Logger log = Logger.getLogger(StreamConfigManager.class.getName());

    public static final Duration DEFAULT_BLOCK = Duration.ofMillis(300);

    public static final OptionalLong DEFAULT_COUNT = OptionalLong.empty();

    private final AbstractRedisClient client;

    private final String key;

    private final JavaPropsMapper mapper;

    private final T config;

    private Duration block = DEFAULT_BLOCK;

    private OptionalLong count = DEFAULT_COUNT;

    private StreamPoller poller;

    private ExecutorService executor;

    public StreamConfigManager(AbstractRedisClient client, String key, T config, JavaPropsMapper mapper) {
        this.client = client;
        this.key = key;
        this.config = config;
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
        StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client);
        List<StreamMessage<String, String>> messages = connection.sync().xrevrange(key, Range.create("-", "+"),
                Limit.create(0, 1));
        if (messages.isEmpty()) {
            Map<String, String> map = mapper.writeValueAsMap(config);
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

    @SuppressWarnings("unchecked")
    @Override
    public void accept(StreamMessage<String, String> message) {
        try {
            T newConfig = (T) mapper.readMapAs(message.getBody(), config.getClass());
            if (newConfig != null) {
                BeanUtils.copyProperties(newConfig, config);
                log.log(Level.INFO, "Updated configuration id {0}: {1}", new Object[] { message.getId(), config });
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Could not parse config", e);
        }
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
    public T get() {
        return config;
    }

    @Override
    public void stop() throws InterruptedException, ExecutionException, TimeoutException {
        poller.stop();
        poller = null;
        executor.shutdown();
        executor = null;
    }

}
