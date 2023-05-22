package com.redis.smartcache.core;

import io.airlift.units.Duration;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class RuleConfig implements Cloneable {

    public static final Duration DEFAULT_TTL = new Duration(1, TimeUnit.HOURS);

    private final PropertyChangeSupport support = new PropertyChangeSupport(this);

    private List<String> tables;
    private List<String> tablesAny;
    private List<String> tablesAll;
    private String regex;

    private List<String> queryIds;
    private Duration ttl = DEFAULT_TTL;

    public RuleConfig() {
    }

    private RuleConfig(RuleConfig.Builder builder) {
        this.tables = builder.tables;
        this.tablesAny = builder.tablesAny;
        this.tablesAll = builder.tablesAll;
        this.regex = builder.regex;
        this.queryIds = builder.queryIds;
        this.ttl = builder.ttl;
    }

    @Override
    public RuleConfig clone(){
        RuleConfig res = new RuleConfig();
        if (tables != null){
            res.tables = new ArrayList<>(this.tables);
        }

        if (tablesAll != null){
            res.tablesAll = new ArrayList<>(this.tablesAll);
        }

        if (tablesAny != null){
            res.tablesAny = new ArrayList<>(this.tablesAny);
        }

        if (queryIds != null){
            res.queryIds = new ArrayList<>(this.queryIds);
        }

        res.regex = this.regex;
        res.ttl = this.ttl;
        return res;
    }

    public void addPropertyChangeListener(PropertyChangeListener listener) {
        support.addPropertyChangeListener(listener);
    }

    public void removePropertyChangeListener(PropertyChangeListener listener) {
        support.removePropertyChangeListener(listener);
    }

    public List<String> getQueryIds() {
        return queryIds;
    }

    public void setQueryIds(List<String> queryIds) {
        support.firePropertyChange("queryIds", this.queryIds, queryIds);
        this.queryIds = queryIds;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        support.firePropertyChange("regex", this.regex, regex);
        this.regex = regex;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        support.firePropertyChange("tables", this.tables, tables);
        this.tables = tables;
    }

    public List<String> getTablesAny() {
        return tablesAny;
    }

    public void setTablesAny(List<String> tablesAny) {
        support.firePropertyChange("tablesAny", this.tablesAny, tablesAny);
        this.tablesAny = tablesAny;
    }

    public List<String> getTablesAll() {
        return tablesAll;
    }

    public void setTablesAll(List<String> tablesAll) {
        support.firePropertyChange("tablesAll", this.tablesAll, tablesAll);
        this.tablesAll = tablesAll;
    }

    /**
     *
     * @return Key expiration duration. Use a duration of zero for no caching
     */
    public Duration getTtl() {
        return ttl;
    }

    public void setTtl(Duration ttl) {
        support.firePropertyChange("ttl", this.ttl, ttl);
        this.ttl = ttl;
    }

    @Override
    public String toString() {
        return "RuleConfig [tables=" + tables + ", tablesAny=" + tablesAny + ", tablesAll=" + tablesAll + ", regex="
                + regex + ", queryIds=" + queryIds + ", ttl=" + ttl + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryIds, regex, tables, tablesAll, tablesAny, ttl);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RuleConfig other = (RuleConfig) obj;
        return Objects.equals(queryIds, other.queryIds) && Objects.equals(regex, other.regex)
                && Objects.equals(tables, other.tables) && Objects.equals(tablesAll, other.tablesAll)
                && Objects.equals(tablesAny, other.tablesAny) && Objects.equals(ttl, other.ttl);
    }

    public static RuleConfig.Builder tables(String... tables) {
        RuleConfig.Builder builder = new RuleConfig.Builder();
        builder.tables = Arrays.asList(tables);
        return builder;
    }

    public static RuleConfig.Builder tablesAny(String... tables) {
        RuleConfig.Builder builder = new RuleConfig.Builder();
        builder.tablesAny = Arrays.asList(tables);
        return builder;
    }

    public static RuleConfig.Builder tablesAll(String... tables) {
        RuleConfig.Builder builder = new RuleConfig.Builder();
        builder.tablesAll = Arrays.asList(tables);
        return builder;
    }

    public static RuleConfig.Builder queryIds(String... ids) {
        RuleConfig.Builder builder = new RuleConfig.Builder();
        builder.queryIds = Arrays.asList(ids);
        return builder;
    }

    public static RuleConfig.Builder regex(String regex) {
        RuleConfig.Builder builder = new RuleConfig.Builder();
        builder.regex = regex;
        return builder;
    }

    public static RuleConfig.Builder passthrough() {
        return new RuleConfig.Builder();
    }

    public static final class Builder {

        private List<String> tables;
        private List<String> tablesAny;
        private List<String> tablesAll;
        private String regex;
        private List<String> queryIds;
        private Duration ttl = DEFAULT_TTL;

        public Builder() {
        }

        public RuleConfig.Builder tables(String... tables) {
            this.tables = Arrays.asList(tables);
            return this;
        }

        public RuleConfig.Builder tablesAny(String... tables) {
            this.tablesAny = Arrays.asList(tables);
            return this;
        }

        public RuleConfig.Builder tablesAll(String... tables) {
            this.tablesAll = Arrays.asList(tables);
            return this;
        }

        public RuleConfig.Builder regex(String regex) {
            this.regex = regex;
            return this;
        }

        public RuleConfig.Builder queryIds(String... ids) {
            this.queryIds = Arrays.asList(ids);
            return this;
        }

        public RuleConfig.Builder ttl(Duration ttl) {
            this.ttl = ttl;
            return this;
        }

        public RuleConfig build() {
            return new RuleConfig(this);
        }
    }

}