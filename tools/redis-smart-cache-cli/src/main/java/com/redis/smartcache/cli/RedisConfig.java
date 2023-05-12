package com.redis.smartcache.cli;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.smartcache.core.ClientManager;
import com.redis.smartcache.core.Config;

public class RedisConfig {

    public RedisConfig(String hostname, String port){
        this.hostname = hostname;
        this.port = port;
    }

    private final String hostname;
    private final String port;


    public Config conf(){
        Config config = new Config();
        Config.RedisConfig conf = new Config.RedisConfig();
        conf.setUri(String.format("redis://%s:%s",hostname,port));
        config.setRedis(conf);
        return config;
    }

    public ClientManager abstractRedisClient(){
        return new ClientManager();
    }


    public StatefulRedisModulesConnection<String, String> modClient(){
        return RedisModulesClient.create(String.format("redis://%s:%s",hostname,port)).connect();
    }
}
