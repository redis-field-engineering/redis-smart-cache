package com.redis.smartcache.cli;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.smartcache.core.ClientManager;
import com.redis.smartcache.core.Config;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

@Configuration
@EnableConfigurationProperties(RedisProperties.class)
public class RedisConfig {
    @Bean
    public Config conf(){
        Config config = new Config();
        Config.RedisConfig conf = new Config.RedisConfig();
        conf.setUri("redis://localhost:6379");
        config.setRedis(conf);
        return config;
    }

    @Bean
    public ClientManager abstractRedisClient(){
        return new ClientManager();
    }

//    @Bean
//    public StatefulRedisConnection<String,String> redisClient() {
//        return RedisClient.create("redis://localhost:6379").connect();
//    }

    @Bean
    public StatefulRedisModulesConnection<String, String> modClient(){
        return RedisModulesClient.create("redis://localhost:6379").connect();
    }
}
