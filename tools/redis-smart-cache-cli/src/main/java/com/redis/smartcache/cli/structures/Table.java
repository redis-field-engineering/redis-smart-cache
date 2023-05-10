package com.redis.smartcache.cli.structures;

import com.redis.smartcache.cli.util.Util;
import com.redis.smartcache.core.Config;

public class Table implements RowStringable {

    private String name;
    private Config.RuleConfig rule;
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

    public Config.RuleConfig getRule() {
        return rule;
    }

    public double getQueryTime() {
        return queryTime;
    }

    public long getAccessFrequency() {
        return accessFrequency;
    }

    public static String headerRow(int colWidth){
        StringBuilder sb = new StringBuilder();
        sb.append("|");
        sb.append(Util.center("Table Name",colWidth));
        sb.append("|");
        sb.append(Util.center("TTL", colWidth));
        sb.append("|");
        sb.append(Util.center("Avg Query Time", colWidth));
        sb.append("|");
        sb.append(Util.center("Access Frequency", colWidth));
        sb.append("|");
        return sb.toString();
    }

    @Override
    public String toRowString(int colWidth) {
        StringBuilder sb = new StringBuilder();
        sb.append("|");
        sb.append(Util.center(getName(),colWidth));
        sb.append("|");
        sb.append(Util.center(this.ttlStr(),colWidth));
        sb.append("|");
        sb.append(Util.center(String.valueOf(getQueryTime()),colWidth));
        sb.append("|");
        sb.append(Util.center(String.valueOf(getAccessFrequency()),colWidth));
        sb.append("|");
        return sb.toString();
    }

    public static class Builder{
        private String name;
        private Config.RuleConfig rule;
        private double queryTime;
        private long accessFrequency;

        public Builder(){
        }

        public Builder name(String name){
            this.name = name;
            return this;
        }

        public Builder rule(Config.RuleConfig rule){
            this.rule = rule;
            return this;
        }

        public Builder queryTime(double queryTime){
            this.queryTime = queryTime;
            return this;
        }

        public Builder accessFrequency(long accessFrequency){
            this.accessFrequency = accessFrequency;
            return this;
        }

        public Table build(){
            Table table = new Table();
            table.name = name;
            table.rule = rule;
            table.queryTime = queryTime;
            table.accessFrequency = accessFrequency;
            return table;
        }
    }
}
