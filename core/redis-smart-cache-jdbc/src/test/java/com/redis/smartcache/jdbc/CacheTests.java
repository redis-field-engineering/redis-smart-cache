package com.redis.smartcache.jdbc;

import java.sql.SQLException;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.smartcache.test.RowSetBuilder;

@Testcontainers
class CacheTests extends AbstractTests {

    @Test
    void oomRetryInterval() throws SQLException {
        redisConnection.sync().configSet("maxmemory", "1mb");
        int rows = 100;
        int columns = 10;
        RowSetBuilder rowSetBuilder = RowSetBuilder.of(new RowSetFactoryImpl()).rowCount(rows).columnCount(columns);
        int initialErrorReplies = errorReplies();
        RedisRowSetCache cache = new RedisRowSetCache(client, new RowSetCodec(100 * 1024 * 1024), Duration.ofMillis(100));
        for (int index = 1; index <= 10; index++) {
            cache.put("key:" + index, rowSetBuilder.build(), 0);
        }
        Assertions.assertEquals(1, errorReplies() - initialErrorReplies);
        redisConnection.sync().configSet("maxmemory", "100mb");
        String successKey = "key:success";
        Awaitility.await().pollDelay(Duration.ofMillis(10)).pollInterval(Duration.ofMillis(10)).atMost(Duration.ofMillis(200))
                .until(() -> {
                    cache.put(successKey, rowSetBuilder.build(), 0);
                    return redisConnection.sync().exists(successKey) == 1;
                });
        Assertions.assertTrue(redisConnection.sync().get(successKey).length() > 100);
        cache.close();
    }

    private int errorReplies() {
        String info = redisConnection.sync().info("stats");
        Matcher matcher = patternFor("total_error_replies").matcher(info);
        matcher.find();
        return Integer.parseInt(matcher.group(1));
    }

    private static Pattern patternFor(String propertyName) {
        return Pattern.compile(String.format("^%s:(.*)$", Pattern.quote(propertyName)), Pattern.MULTILINE);
    }

}
