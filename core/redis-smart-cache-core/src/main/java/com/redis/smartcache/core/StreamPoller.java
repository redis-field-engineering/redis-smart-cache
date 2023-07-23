package com.redis.smartcache.core;

import java.util.function.Consumer;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;

public class StreamPoller implements Runnable {

    private final StatefulRedisModulesConnection<String, String> connection;

    private final XReadArgs xreadArgs;

    private final StreamOffset<String> offset;

    private final Consumer<StreamMessage<String, String>> consumer;

    private boolean stop;

    private State state = State.STARTING;

    public StreamPoller(StatefulRedisModulesConnection<String, String> connection, XReadArgs xreadArgs,
            StreamOffset<String> offset, Consumer<StreamMessage<String, String>> consumer) {
        this.connection = connection;
        this.xreadArgs = xreadArgs;
        this.offset = offset;
        this.consumer = consumer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        this.state = State.STARTED;
        while (!stop) {
            connection.sync().xread(xreadArgs, offset).forEach(consumer);
        }
        this.state = State.STOPPED;
    }

    public void stop() {
        stop = true;
        this.state = State.STOPPING;
    }

    public State getState() {
        return state;
    }

}
