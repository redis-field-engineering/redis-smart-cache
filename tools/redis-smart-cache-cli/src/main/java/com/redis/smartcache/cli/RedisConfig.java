package com.redis.smartcache.cli;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.smartcache.core.ClientManager;
import com.redis.smartcache.core.Config;

public class RedisConfig {

    public RedisConfig(String hostname, String port, String applicationName){
        this.hostname = hostname;
        this.port = port;
        this.applicationName = applicationName;
    }

    private final String hostname;
    private final String port;

    public String getApplicationName() {
        return applicationName;
    }

    private final String applicationName;


    public Config conf(){
        Config config = new Config();
        Config.RedisConfig conf = new Config.RedisConfig();
        conf.setUri(String.format("redis://%s:%s",hostname,port));
        config.setRedis(conf);
        config.setName(applicationName);
        return config;
    }

    public ClientManager abstractRedisClient(){
        return new ClientManager();
    }


    public StatefulRedisModulesConnection<String, String> modClient(){
        return RedisModulesClient.create(String.format("redis://%s:%s",hostname,port)).connect();
    }
}
