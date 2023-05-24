package com.redis.smartcache.core;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.airlift.units.Duration;

public class RuleConfig implements Cloneable {

	public static final Duration TTL_NO_CACHING = Duration.succinctNanos(0);
	public static final Duration DEFAULT_TTL = TTL_NO_CACHING;

	private final PropertyChangeSupport support = new PropertyChangeSupport(this);

	private List<String> tables;
	private List<String> tablesAny;
	private List<String> tablesAll;
	private String regex;
	private List<String> queryIds;
	private Duration ttl = TTL_NO_CACHING;

	public RuleConfig() {
	}

	public RuleConfig(RuleConfig source) {
		if (source.tables != null) {
			this.tables = new ArrayList<>(source.tables);
		}
		if (source.tablesAll != null) {
			this.tablesAll = new ArrayList<>(source.tablesAll);
		}
		if (tablesAny != null) {
			this.tablesAny = new ArrayList<>(source.tablesAny);
		}
		if (queryIds != null) {
			this.queryIds = new ArrayList<>(source.queryIds);
		}
		this.regex = source.regex;
		this.ttl = source.ttl;
	}

	private RuleConfig(Builder builder) {
		this.tables = builder.tables;
		this.tablesAny = builder.tablesAny;
		this.tablesAll = builder.tablesAll;
		this.regex = builder.regex;
		this.queryIds = builder.queryIds;
		this.ttl = builder.ttl;
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

	@Override
	public RuleConfig clone() {
		return new RuleConfig(this);
	}

	public static Builder tables(String... tables) {
		Builder builder = new Builder();
		builder.tables = Arrays.asList(tables);
		return builder;
	}

	public static Builder tablesAny(String... tables) {
		Builder builder = new Builder();
		builder.tablesAny = Arrays.asList(tables);
		return builder;
	}

	public static Builder tablesAll(String... tables) {
		Builder builder = new Builder();
		builder.tablesAll = Arrays.asList(tables);
		return builder;
	}

	public static Builder queryIds(String... ids) {
		Builder builder = new Builder();
		builder.queryIds = Arrays.asList(ids);
		return builder;
	}

	public static Builder regex(String regex) {
		Builder builder = new Builder();
		builder.regex = regex;
		return builder;
	}

	public static Builder passthrough() {
		return new Builder();
	}

	public static final class Builder {

		private List<String> tables;
		private List<String> tablesAny;
		private List<String> tablesAll;
		private String regex;
		private List<String> queryIds;
		private Duration ttl = DEFAULT_TTL;

		private Builder() {
		}

		public Builder tables(String... tables) {
			this.tables = Arrays.asList(tables);
			return this;
		}

		public Builder tablesAny(String... tables) {
			this.tablesAny = Arrays.asList(tables);
			return this;
		}

		public Builder tablesAll(String... tables) {
			this.tablesAll = Arrays.asList(tables);
			return this;
		}

		public Builder regex(String regex) {
			this.regex = regex;
			return this;
		}

		public Builder queryIds(String... ids) {
			this.queryIds = Arrays.asList(ids);
			return this;
		}

		public Builder ttl(Duration ttl) {
			this.ttl = ttl;
			return this;
		}

		public RuleConfig build() {
			return new RuleConfig(this);
		}
	}

}