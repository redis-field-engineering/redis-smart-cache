package com.redis.smartcache.cli;

import com.redis.smartcache.cli.structures.QueryInfo;
import com.redis.smartcache.cli.structures.TableInfo;
import com.redis.smartcache.core.Config;
import com.redis.smartcache.core.RuleConfig;

import java.util.List;

public interface RedisService {
    List<QueryInfo> getQueries();
    void commitRules(List<RuleConfig> rules);
    List<RuleConfig> getRules();
    List<TableInfo> getTables();
}
