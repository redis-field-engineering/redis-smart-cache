package com.redis.smartcache.core;

import java.util.Set;

public class Query {

    private String id;

    private String sql;

    private Set<String> tables;

    public Query() {
    }

    private Query(QueryBuilder builder) {
        this.id = builder.id;
        this.sql = builder.sql;
        this.tables = builder.tables;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Set<String> getTables() {
        return tables;
    }

    public void setTables(Set<String> tables) {
        this.tables = tables;
    }

    public static class QueryBuilder {

        private String id;

        private String sql;

        private Set<String> tables;

        public QueryBuilder() {
        }

        public QueryBuilder setId(String id) {
            this.id = id;
            return this;
        }

        public QueryBuilder setSql(String sql) {
            this.sql = sql;
            return this;
        }

        public QueryBuilder setTables(Set<String> tables) {
            this.tables = tables;
            return this;
        }

        public Query build() {
            return new Query(this);
        }

    }

}
