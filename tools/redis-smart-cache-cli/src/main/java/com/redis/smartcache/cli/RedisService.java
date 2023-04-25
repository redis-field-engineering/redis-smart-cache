package com.redis.smartcache.cli;

import com.redis.smartcache.cli.structures.QueryInfo;

import java.util.List;

public interface RedisService {
    String ping();
    List<QueryInfo> getQueries(String applicationName);

}
