package com.redis.smartcache.cli.structures;

public enum SortDirection {
    desc("desc"),
    asc("asc");

    final private String value;

    SortDirection(String value){
        this.value = value;
    }

    public String getValue(){
        return value;
    }
}
