package com.redis.smartcache.jdbc;

import java.util.Map.Entry;

public class CallableParameter {

    private final String key;

    private final Object value;

    public CallableParameter(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public CallableParameter(Entry<String, Object> entry) {
        this(entry.getKey(), entry.getValue());
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }

}
