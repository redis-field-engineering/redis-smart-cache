package com.redis.smartcache.cli.structures;

import com.redis.lettucemod.search.Document;
import com.redis.smartcache.core.RuleConfig;
import com.redis.smartcache.core.Query;
import com.redis.smartcache.cli.util.Util;

import java.util.*;

public class QueryInfo implements RowInfo {
    private final Query query;

    private RuleConfig currentRule;
    private RuleConfig pendingRule;

    public long getCount() {
        return count;
    }

    public double getMeanQueryTime() {
        return meanQueryTime;
    }

    private final long count;
    private final double meanQueryTime;


    private QueryInfo(Builder builder) {
        this.query = builder.query;
        this.currentRule = builder.currentRule;
        this.pendingRule = builder.pendingRule;
        this.count = builder.count;
        this.meanQueryTime = builder.meanQueryTime;
    }


    public Query getQuery(){
        return this.query;
    }


    public String getQueryId(){
        if (query == null){
            return "";
        }
        return query.getId();
    }

    public String getQueryTablesString(){
        if(query ==null){
            return "";
        }

        return String.join(",", query.getTables());
    }

    public String getQuerySql(){
        if (query == null){
            return "";
        }

        return query.getSql();
    }

    public boolean getIsCached(){
        return currentRule != null && !Objects.equals(currentRule.getTtl().toString(), "0s");
    }

    public String getCurrentTtlString(){
        if (currentRule == null){
            return "";
        }

        return currentRule.getTtl().toString();
    }

    public String getPendingRuleTtlString(){
        if (pendingRule == null){
            return "";
        }

        return pendingRule.getTtl().toString();
    }

    public void setPendingRule(RuleConfig rule){
        this.pendingRule = rule;
    }

    public void setCurrentRule(RuleConfig rule){
        this.currentRule = rule;
    }

    public static int compare(QueryInfo first, QueryInfo second, SortDirection sortDirection, SortField sortBy){
        switch(sortBy){
            case accessFrequency:
                if(sortDirection == SortDirection.asc){

                    return Long.compare(first.getCount(), second.getCount());
                }
                return Long.compare(second.getCount(), first.getCount());
            case queryTime:
                if(sortDirection == SortDirection.asc){
                    return Double.compare(first.getMeanQueryTime(), second.getMeanQueryTime());
                }
                return Double.compare(second.getMeanQueryTime(), first.getMeanQueryTime());
            case tables:
                if(sortDirection == SortDirection.asc){
                    return first.getQueryTablesString().compareTo(second.getQueryTablesString());
                }
                return second.getQueryTablesString().compareTo(first.getQueryTablesString());
            case id:
                if (sortDirection == SortDirection.asc) {
                    return first.getQueryId().compareTo(second.getQueryId());
                }
                return second.getQueryId().compareTo(first.getQueryId());
        }
        return Long.compare(first.getCount(), second.getCount());
    }

    public static String getHeaderRow(int colWidth, boolean includePending){
        StringBuilder sb = new StringBuilder();
        sb.append("|");
        sb.append(Util.center("Id",colWidth));
        sb.append("|");
        sb.append(Util.center("SQL", colWidth));
        sb.append("|");
        sb.append(Util.center("Tables", colWidth));
        sb.append("|");
        sb.append(Util.center("Is Cached", colWidth));
        sb.append("|");
        sb.append(Util.center("Current TTL", colWidth));
        sb.append("|");
        if(includePending){
            sb.append(Util.center("Pending TTL", colWidth));
            sb.append("|");
        }

        sb.append(Util.center("Access Frequency", colWidth));
        sb.append("|");
        sb.append(Util.center("Mean Query Time", colWidth));
        sb.append("|");
        return sb.toString();
    }

    public String toRowString(int colWidth){
        return toRowString(colWidth, true);
    }

    public String toRowString(int colWidth, boolean includePending){
        StringBuilder sb = new StringBuilder();
        sb.append("|");
        sb.append(Util.center(getQueryId(),colWidth));
        sb.append("|");
        sb.append(Util.center(getQuerySql(),colWidth));
        sb.append("|");
        sb.append(Util.center(getQueryTablesString(),colWidth));
        sb.append("|");
        sb.append(Util.center(String.valueOf(getIsCached()),colWidth));
        sb.append("|");
        sb.append(Util.center(getCurrentTtlString(),colWidth));
        sb.append("|");
        if(includePending){
            sb.append(Util.center(getPendingRuleTtlString(),colWidth));
            sb.append("|");
        }

        sb.append(Util.center(String.valueOf(count),colWidth));
        sb.append("|");
        sb.append(Util.center(String.format("%.3fms", meanQueryTime),colWidth));
        sb.append("|");
        return sb.toString();
    }
    public String toFormattedString(int width){
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Query id: %s%n", query.getId()));
        if(query.getSql().length()+11 > width){
            List<String> substrings = Util.chopString(query.getSql(), width);
            sb.append("Query sql:\n");
            for(String s : substrings){
                sb.append(String.format("%s%n",s));
            }
        }
        else{
            sb.append(String.format("Query sql: %s%n", query.getSql()));
        }

        sb.append(String.format("Query tables: %s%n", query.getTables()));
        if (currentRule!=null){
            sb.append(String.format("Current TTL: %s%n", currentRule.getTtl()));
        }
        else{
            sb.append(String.format("Current TTL:%n"));
        }

        if (pendingRule!=null){
            sb.append(String.format("Pending TTL: %s%n", pendingRule.getTtl()));
        }
        else{
            sb.append(String.format("Pending TTL:%n"));
        }

        return sb.toString();
    }

    //builder

    public static class Builder {
        private Query query;
        private RuleConfig currentRule;
        private RuleConfig pendingRule;
        private long count;
        private double meanQueryTime;

        public Builder() {}

        public void setQuery(Query query) {
            this.query = query;
        }

        public void setCurrentRule(RuleConfig currentRule) {
            this.currentRule = currentRule;
        }

        public void setPendingRule(RuleConfig pendingRule) {
            this.pendingRule = pendingRule;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public void setMeanQueryTime(double meanQueryTime) {
            this.meanQueryTime = meanQueryTime;
        }

        public QueryInfo build() {
            return new QueryInfo(this);
        }
    }

    public static Optional<RuleConfig> matchRule(Query q, List<RuleConfig> rules){

        for (RuleConfig rule : rules){
            if (rule.getTables() == null && rule.getQueryIds() == null && rule.getTablesAll() == null && rule.getTablesAny() == null && rule.getRegex() == null) {
                return Optional.of(rule); // this rule is a rule with empty matches - so it matches anything

            }

            if (rule.getQueryIds() != null && !rule.getQueryIds().contains(q.getId()))
            {
                continue;
            }

            if(rule.getTables() != null && !q.getTables().equals(new HashSet<>(rule.getTables()))){
                continue;
            }

            if(rule.getTablesAll() != null && !q.getTables().containsAll(rule.getTablesAll())){
                continue;
            }

            if(rule.getTablesAny() != null && q.getTables().stream().noneMatch(x->rule.getTablesAny().contains(x))){
                continue;
            }

            if(rule.getRegex() != null && !q.getSql().matches(rule.getRegex())){
                continue;
            }

            return Optional.of(rule);
        }

        return Optional.empty();

    }

    public static QueryInfo fromDocument(Document<String, String> doc){
        Query query = new Query.QueryBuilder()
                .setId(doc.get("id"))
                .setSql(doc.get("sql"))
                .setTables(new HashSet<>(Arrays.asList(doc.get("table").split(","))))
                .build();

        Builder builder = new Builder();
        builder.setQuery(query);
        if(doc.containsKey("count")){
            builder.setCount(Long.parseLong(doc.get("count")));
        }

        if(doc.containsKey("mean")){
            builder.setMeanQueryTime(Double.parseDouble(doc.get("mean")));
        }

        return builder.build();
    }

}
