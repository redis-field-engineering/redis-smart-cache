package com.redis.smartcache.cli.structures;

public enum RuleType {
    TABLES("Tables"),
    TABLES_ANY("Tables Any"),
    TABLES_ALL("Tables ALl"),
    REGEX("Regex"),
    QUERY_IDS("Query IDs");


    final private String value;

    RuleType(String value){
        this.value = value;
    }

    public String getValue(){
        return value;
    }

}
