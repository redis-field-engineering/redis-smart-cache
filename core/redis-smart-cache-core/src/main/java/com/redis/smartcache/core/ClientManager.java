package com.redis.smartcache.core;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisURIBuilder;
import com.redis.smartcache.core.config.RedisConfig;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class ClientManager implements AutoCloseable {

    private static final Logger log = Logger.getLogger(ClientManager.class.getName());

    private final Map<RedisConfig, AbstractRedisClient> clients = new HashMap<>();

    public AbstractRedisClient getClient(RedisConfig config) {
        return clients.computeIfAbsent(config, this::createClient);
    }

    private AbstractRedisClient createClient(RedisConfig config) {
        RedisURI redisURI = redisURI(config);
        log.log(Level.FINE, "Creating Redis client with URI {0}", redisURI);
        return ClientBuilder.create(redisURI).cluster(config.isCluster()).build();
    }

    private RedisURI redisURI(RedisConfig config) {
        RedisURIBuilder builder = RedisURIBuilder.create();
        builder.uri(config.getUri());
        builder.username(config.getUsername());
        builder.password(config.getPassword());
        builder.ssl(config.isTls());
        builder.sslVerifyMode(config.getTlsVerify());
        return builder.build();
    }

    @Override
    public void close() {
        clients.values().forEach(c -> {
            c.shutdown();
            c.getResources().shutdown();
        });
        clients.clear();
    }

}
