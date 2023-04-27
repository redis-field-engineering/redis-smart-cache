package com.redis.smartcache.cli;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.SearchResults;
import com.redis.smartcache.cli.structures.QueryInfo;
import com.redis.smartcache.core.ClientManager;
import com.redis.smartcache.core.Config;
import com.redis.smartcache.core.Config.RuleConfig;
import com.redis.smartcache.core.RulesetManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class RedisServiceImpl implements RedisService{
    @Autowired
    Config conf;

    @Autowired
    ClientManager manager;

    @Autowired
    StatefulRedisModulesConnection<String, String> connection;


    public String ping(){
        return connection.sync().ping();
    }

    public List<RuleConfig> getRules(){
        RulesetManager rulesetManager = new RulesetManager(manager);

        Config.RulesetConfig ruleSetConfig = rulesetManager.getRuleset(conf);
        return ruleSetConfig.getRules();
    }

    static String configKeyName(String applicationName){
        return String.format("%s:config", applicationName);
    }

    static String HashKeyName(String applicationName, String id){
        return String.format("%s:query:%s", applicationName, id);

    }

    static String IndexName(String applicationName){
        return String.format("%s-query-idx", applicationName);
    }

    public List<QueryInfo> getQueries(String applicationName){
        List<QueryInfo> response = new ArrayList<>();
        List<RuleConfig> rules = getRules();

        RediSearchCommands<String, String> searchCommands = connection.sync();

        SearchResults<String, String> searchResults = searchCommands.ftSearch(IndexName(applicationName), "*");

        for(Document<String, String> doc : searchResults){

            QueryInfo qi = QueryInfo.fromDocument(doc);
            Optional<RuleConfig> currentRule = QueryInfo.matchRule(qi.getQuery(), rules);
            currentRule.ifPresent(qi::setCurrentRule);
            response.add(qi);

        }
        return response;
    }

    public void commitNewRules(List<RuleConfig> rules){
        conf.getRuleset().setRules(rules);

    }
}
