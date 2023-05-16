package com.redis.smartcache.cli.structures;

import com.redis.smartcache.cli.util.Util;
import com.redis.smartcache.core.Config;

public class RuleInfo implements RowInfo {
    public enum Status{
        Current, Editing, New, Delete
    }

    public Config.RuleConfig getRule() {
        return rule;
    }

    public void setRule(Config.RuleConfig rule) {
        this.rule = rule;
    }

    Config.RuleConfig rule;

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    Status status;

    public RuleInfo(Config.RuleConfig rule, Status status){
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
        StringBuilder sb = new StringBuilder();
        sb.append("|");
        sb.append(Util.center(ruleType().getValue(), colWidth));
        sb.append("|");
        sb.append(Util.center(ruleMatch(), colWidth));
        sb.append("|");
        sb.append(Util.center(rule.getTtl().toString(),colWidth));
        sb.append("|");
        sb.append(Util.center(status.toString(),colWidth));
        sb.append("|");
        return sb.toString();
    }

    public static String getHeaderRow(int colWidth){
        StringBuilder sb = new StringBuilder();
        sb.append("|");
        sb.append(Util.center("Type",colWidth));
        sb.append("|");
        sb.append(Util.center("Match", colWidth));
        sb.append("|");
        sb.append(Util.center("TTL", colWidth));
        sb.append("|");
        sb.append(Util.center("Status", colWidth));
        sb.append("|");
        return sb.toString();
    }
}
