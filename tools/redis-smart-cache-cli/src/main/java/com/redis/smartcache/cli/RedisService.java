package com.redis.smartcache.cli;

import com.redis.smartcache.cli.structures.QueryInfo;
import com.redis.smartcache.core.Config;
import com.redis.smartcache.cli.structures.Table;

import java.util.List;

public interface RedisService {
    String ping();
    List<QueryInfo> getQueries(String applicationName);
    void commitRules(List<Config.RuleConfig> rules);
    List<Config.RuleConfig> getRules();
    List<Table> getTables();
}
