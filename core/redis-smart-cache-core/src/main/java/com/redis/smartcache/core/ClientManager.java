package com.redis.smartcache.core;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.smartcache.core.config.RedisConfig;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientManager implements AutoCloseable {

    private static final Logger log = Logger.getLogger(ClientManager.class.getName());

    private final Map<RedisConfig, AbstractRedisClient> clients = new HashMap<>();

    public AbstractRedisClient getClient(RedisConfig config) {
        return clients.computeIfAbsent(config, this::createClient);
    }

    private AbstractRedisClient createClient(RedisConfig config) {
        RedisURI redisURI = buildRedisURI(config);
        log.log(Level.FINE, "Creating Redis client with URI {0}", redisURI);
        if(config.isCluster()){
            return RedisModulesClusterClient.create(redisURI);
        }
        else{
            return RedisModulesClient.create(redisURI);
        }
    }

    private RedisURI buildRedisURI(RedisConfig config){
        RedisURI startingUri = RedisURI.create(config.getUri());

        RedisURI.Builder builder = RedisURI.builder(startingUri);

        if(StringUtils.hasLength(config.getUsername()) && config.getPassword() != null && config.getPassword().length > 0){
            builder.withAuthentication(config.getUsername(), config.getPassword());
        }
        else if(config.getPassword() != null && config.getPassword().length > 0){
            builder.withPassword(config.getPassword());
        }

        if(config.isTls()){
            builder.withSsl(config.isTls());
            builder.withVerifyPeer(config.getTlsVerify());
        }

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
