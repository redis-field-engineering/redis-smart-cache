package com.redis.smartcache.cli.structures;

import com.redis.smartcache.cli.util.Util;
import com.redis.smartcache.core.Config;
import com.redis.smartcache.core.RuleConfig;

public class TableInfo implements RowInfo {

    private String name;
    private RuleConfig rule;
    private double queryTime;
    private long accessFrequency;

    public String ttlStr(){
        if(rule != null){
            return rule.getTtl().toString();
        }
        return "";
    }

    public String getName() {
        return name;
    }

    public RuleConfig getRule() {
        return rule;
    }

    public double getQueryTime() {
        return queryTime;
    }

    public long getAccessFrequency() {
        return accessFrequency;
    }

    public static String headerRow(int colWidth){
        return "|" +
                Util.center("Table Name", colWidth) +
                "|" +
                Util.center("TTL", colWidth) +
                "|" +
                Util.center("Avg Query Time", colWidth) +
                "|" +
                Util.center("Access Frequency", colWidth) +
                "|";
    }

    @Override
    public String toRowString(int colWidth) {
        return "|" +
                Util.center(getName(), colWidth) +
                "|" +
                Util.center(this.ttlStr(), colWidth) +
                "|" +
                Util.center(String.format("%.3fms", getQueryTime()), colWidth) +
                "|" +
                Util.center(String.valueOf(getAccessFrequency()), colWidth) +
                "|";
    }

    public static class Builder{
        private String name;
        private RuleConfig rule;
        private double queryTime;
        private long accessFrequency;

        public Builder(){
        }

        public Builder name(String name){
            this.name = name;
            return this;
        }

        public void rule(RuleConfig rule){
            this.rule = rule;
        }

        public Builder queryTime(double queryTime){
            this.queryTime = queryTime;
            return this;
        }

        public Builder accessFrequency(long accessFrequency){
            this.accessFrequency = accessFrequency;
            return this;
        }

        public TableInfo build(){
            TableInfo tableInfo = new TableInfo();
            tableInfo.name = name;
            tableInfo.rule = rule;
            tableInfo.queryTime = queryTime;
            tableInfo.accessFrequency = accessFrequency;
            return tableInfo;
        }
    }
}
