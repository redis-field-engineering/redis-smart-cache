package com.redis.smartcache.core.config;

import java.util.concurrent.TimeUnit;

import com.redis.smartcache.core.KeyBuilder;

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;

public class CacheConfig {

    public static final DataSize DEFAULT_BUFFER_CAPACITY = DataSize.of(10, Unit.MEGABYTE);

    private static final Duration DEFAULT_OOM_RETRY_INTERVAL = new Duration(1, TimeUnit.MINUTES);

    private DataSize codecBufferCapacity = DEFAULT_BUFFER_CAPACITY;

    private String keySeparator = KeyBuilder.DEFAULT_SEPARATOR;

    private Duration oomRetryInterval = DEFAULT_OOM_RETRY_INTERVAL;

    private RedisConfig redis;

    /**
     * 
     * @return max byte buffer capacity in bytes
     */
    public DataSize getCodecBufferCapacity() {
        return codecBufferCapacity;
    }

    public void setCodecBufferCapacity(DataSize size) {
        this.codecBufferCapacity = size;
    }

    public void setCodecBufferSizeInBytes(long size) {
        this.codecBufferCapacity = DataSize.ofBytes(size);
    }

    public Duration getOomRetryInterval() {
        return oomRetryInterval;
    }

    public void setOomRetryInterval(Duration oomRetryInterval) {
        this.oomRetryInterval = oomRetryInterval;
    }

    public RedisConfig getRedis() {
        return redis;
    }

    public void setRedis(RedisConfig redis) {
        this.redis = redis;
    }

    public String getKeySeparator() {
        return keySeparator;
    }

    public void setKeySeparator(String keySeparator) {
        this.keySeparator = keySeparator;
    }

}
