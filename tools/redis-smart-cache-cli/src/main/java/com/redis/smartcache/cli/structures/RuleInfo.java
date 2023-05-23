package com.redis.smartcache.cli.structures;

import com.redis.smartcache.cli.util.Util;
import com.redis.smartcache.core.Config;
import com.redis.smartcache.core.RuleConfig;

public class RuleInfo implements RowInfo {
    public enum Status{
        Current, Editing, New, Delete
    }

    public RuleConfig getRule() {
        return rule;
    }

    public void setRule(RuleConfig rule) {
        this.rule = rule;
    }

    RuleConfig rule;

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    Status status;

    public RuleInfo(RuleConfig rule, Status status){
        this.rule = rule;
        this.status = status;
    }

    public RuleType ruleType(){
        if(rule.getTablesAny() != null){
            return RuleType.TABLES_ANY;
        }

        if(rule.getTables() != null){
            return RuleType.TABLES;
        }

        if(rule.getTablesAll() != null){
            return RuleType.TABLES_ALL;
        }

        if(rule.getRegex() != null){
            return RuleType.REGEX;
        }

        if(rule.getQueryIds() != null){
            return RuleType.QUERY_IDS;
        }

        return RuleType.ANY;
    }

    public String ruleMatch(){
        if(rule.getTablesAny() != null){
            return String.join(",", rule.getTablesAny());
        }

        if(rule.getTables() != null){
            return String.join(",", rule.getTables());
        }

        if(rule.getTablesAll() != null){
            return String.join(",", rule.getTablesAll());
        }

        if(rule.getRegex() != null){
            return String.join(",", rule.getRegex());
        }

        if(rule.getQueryIds() != null){
            return String.join(",", rule.getQueryIds());
        }

        return "";
    }

    @Override
    public String toRowString(int colWidth) {
        return "|" +
                Util.center(ruleType().getValue(), colWidth) +
                "|" +
                Util.center(ruleMatch(), colWidth) +
                "|" +
                Util.center(rule.getTtl().toString(), colWidth) +
                "|" +
                Util.center(status.toString(), colWidth) +
                "|";
    }

    public static String getHeaderRow(int colWidth){
        return "|" +
                Util.center("Type", colWidth) +
                "|" +
                Util.center("Match", colWidth) +
                "|" +
                Util.center("TTL", colWidth) +
                "|" +
                Util.center("Status", colWidth) +
                "|";
    }
}
