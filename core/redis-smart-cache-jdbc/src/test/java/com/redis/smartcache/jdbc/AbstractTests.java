package com.redis.smartcache.jdbc;

import java.util.concurrent.Callable;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.testcontainers.RedisStackContainer;

@Testcontainers
abstract class AbstractTests {

    @Container
    protected static final RedisStackContainer redis = new RedisStackContainer(
            RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

    private static final java.time.Duration DEFAULT_AWAIT_TIMEOUT = java.time.Duration.ofHours(3);

    protected RedisModulesClient client;

    protected StatefulRedisModulesConnection<String, String> redisConnection;

    @BeforeEach
    void setupRedisClient() {
        client = RedisModulesClient.create(redis.getRedisURI());
        redisConnection = client.connect();
        redisConnection.sync().flushall();
        awaitUntil(() -> redisConnection.sync().dbsize() == 0);
    }

    @AfterEach
    void teardownRedisClient() throws Exception {
        redisConnection.close();
        client.shutdown();
        client.getResources().shutdown();
    }

    protected static void awaitUntil(Callable<Boolean> callable) {
        Awaitility.await().timeout(DEFAULT_AWAIT_TIMEOUT).until(callable);
    }

}
