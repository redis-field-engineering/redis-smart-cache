package com.redis.smartcache.cli;

import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RediSearchAsyncCommands;
import com.redis.lettucemod.search.*;
import com.redis.smartcache.cli.structures.QueryInfo;
import com.redis.smartcache.cli.structures.TableInfo;
import com.redis.smartcache.core.*;
import com.redis.smartcache.core.rules.Rule;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.internal.Futures;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.redis.smartcache.core.RuleSessionManager.KEY_CONFIG;

//@Service
public class RedisServiceImpl implements RedisService{
    Config conf;

    ClientManager manager;

    StatefulRedisModulesConnection<String, String> connection;

    StreamConfigManager<RulesetConfig> configManager;

    private final JavaPropsMapper mapper = Mappers.propsMapper();

    public RedisServiceImpl(RedisConfig config){
        conf = config.conf();
        manager = config.abstractRedisClient();
        connection = config.modClient();
        RulesetConfig ruleset = conf.getRuleset();
        String key = KeyBuilder.of(conf).build(KEY_CONFIG);
        configManager = new StreamConfigManager<>(manager.getClient(conf), key, ruleset, mapper);
        try{
            configManager.start();
        } catch (IOException e){
            throw new IllegalStateException("Could not start Redis Service", e);
        }
    }


    public String ping(){
        return connection.sync().ping();
    }

    public List<RuleConfig> getRules(){
        RulesetConfig ruleset = conf.getRuleset();

        return Arrays.asList(ruleset.getRules());
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

    public List<QueryInfo> getQueries(){
        List<QueryInfo> response = new ArrayList<>();
        List<RuleConfig> rules = getRules();

        SearchResults<String, String> searchResults = connection.sync().ftSearch(IndexName(conf.getName()), "*");

        for(Document<String, String> doc : searchResults){

            QueryInfo qi = QueryInfo.fromDocument(doc);
            Optional<RuleConfig> currentRule = QueryInfo.matchRule(qi.getQuery(), rules);
            currentRule.ifPresent(qi::setCurrentRule);
            response.add(qi);

        }
        return response;
    }

    public void commitRules(List<RuleConfig> rules){
        Map<String, List<RuleConfig>> map = new HashMap<>();
        map.put("rules",rules); // TODO: Without this the rules are serialized as ruleNum.attribute.etc - there must be a better way to get it to serialize correctly to rules.ruleNum.attribute.etc
        JavaPropsMapper mapper = Mappers.propsMapper();
        try{
            Properties props = mapper.writeValueAsProperties(map);
            List<String> listArgs = new ArrayList<>();

            for(Object o : props.keySet().stream().sorted().collect(Collectors.toList())){
                listArgs.add((String)o);
                listArgs.add((String)props.get(o));
            }

            String key = KeyBuilder.of(conf).build(KEY_CONFIG);
            connection.sync().xadd(key,listArgs.toArray());
        } catch (IOException ignored){

        }
    }

    public List<TableInfo> getTables(){

        List<RuleConfig> rules = getRules();
        List<TableInfo> tableInfos = new ArrayList<>();
        String[] groupStrs = {"name"};
        Reducer[] reducers = {new Reducers.Sum.Builder("count").as("accessFrequency").build(), new Reducers.Avg.Builder("mean").as("avgQueryTime").build()};
        AggregateOptions<String,String> options = AggregateOptions.<String,String>builder().operation(new Apply<String,String>("split(@table, ',')", "name")).operation(new Group(groupStrs, reducers)).build();
        AggregateResults<String> res = connection.sync().ftAggregate("smartcache-query-idx", "*", options);
        for(Map<String,Object> item : res){
            String name = item.get("name").toString();
            double avgQueryTime = Double.parseDouble(item.get("avgQueryTime").toString());
            long accessFrequency = Long.parseLong(item.get("accessFrequency").toString());
            Optional<RuleConfig> rule = rules.stream().filter(x->x.getTablesAny() != null && x.getTablesAny().contains(name)).findAny();
            TableInfo.Builder builder = new TableInfo.Builder().name(name).accessFrequency(accessFrequency).queryTime(avgQueryTime);
            rule.ifPresent(builder::rule);

            tableInfos.add(builder.build());
        }

        return tableInfos;
    }
}
