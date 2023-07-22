package com.redis.smartcache.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KeyBuilderTests {

    @Test
    void createKeysInKeyspace() {
        KeyBuilder builder = KeyBuilder.of("smartcache");
        Assertions.assertEquals("smartcache:metrics", builder.build("metrics"));
        Assertions.assertEquals("smartcache:cache:123", builder.sub("cache").build("123"));
        Assertions.assertEquals("smartcache:cache:123", builder.sub("cache").build(123));
        Assertions.assertEquals("metrics", builder.noKeyspace().build("metrics"));
        Assertions.assertEquals("metrics:123", builder.noKeyspace().build("metrics", "123"));
    }

    @Test
    void createKeysNoKeyspace() {
        Assertions.assertEquals("metrics", KeyBuilder.create().build("metrics"));
        Assertions.assertEquals("cache:123", KeyBuilder.create().sub("cache").build("123"));
        Assertions.assertEquals("cache:123", KeyBuilder.create().sub("cache").build(123));
        Assertions.assertEquals("metrics", KeyBuilder.create().noKeyspace().build("metrics"));
        Assertions.assertEquals("metrics:123", KeyBuilder.create().noKeyspace().build("metrics", "123"));
    }

}
