package com.redis.smartcache.cli;

import com.redis.smartcache.cli.structures.QueryInfo;
import com.redis.smartcache.cli.structures.TableInfo;
import com.redis.smartcache.core.Config;

import java.util.List;

public interface RedisService {
    List<QueryInfo> getQueries();
    void commitRules(List<Config.RuleConfig> rules);
    List<Config.RuleConfig> getRules();
    List<TableInfo> getTables();
}
