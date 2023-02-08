package com.redis.smartcache;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.Arrays;
import java.util.List;

public class RulesetConfig {

	public static final String PROPERTY_RULESET = "ruleset";

	private List<RuleConfig> rules;

	public RulesetConfig() {
		this(RuleConfig.passthrough().build());
	}

	public RulesetConfig(RuleConfig... rules) {
		this.rules = Arrays.asList(rules);
	}

	private final PropertyChangeSupport support = new PropertyChangeSupport(this);

	public void addPropertyChangeListener(PropertyChangeListener listener) {
		support.addPropertyChangeListener(listener);
		rules.forEach(r -> r.addPropertyChangeListener(listener));
	}

	public void removePropertyChangeListener(PropertyChangeListener listener) {
		support.removePropertyChangeListener(listener);
		rules.forEach(r -> r.removePropertyChangeListener(listener));
	}

	public List<RuleConfig> getRules() {
		return rules;
	}

	public void setRules(List<RuleConfig> rules) {
		support.firePropertyChange(PROPERTY_RULESET, this.rules, rules);
		this.rules = rules;
	}

	public static RulesetConfig of(RuleConfig... rules) {
		return new RulesetConfig(rules);
	}

	public static class RuleConfig {

		public static final long DEFAULT_TTL = 3600;

		private final PropertyChangeSupport support = new PropertyChangeSupport(this);

		private String[] tables;
		private String[] tablesAny;
		private String[] tablesAll;
		private String regex;
		private long ttl = DEFAULT_TTL;

		public RuleConfig() {
		}

		private RuleConfig(Builder builder) {
			this.tables = builder.tables;
			this.tablesAny = builder.tablesAny;
			this.tablesAll = builder.tablesAll;
			this.regex = builder.regex;
			this.ttl = builder.ttl;
		}

		public void addPropertyChangeListener(PropertyChangeListener listener) {
			support.addPropertyChangeListener(listener);
		}

		public void removePropertyChangeListener(PropertyChangeListener listener) {
			support.removePropertyChangeListener(listener);
		}

		public String getRegex() {
			return regex;
		}

		public void setRegex(String regex) {
			support.firePropertyChange("regex", this.regex, regex);
			this.regex = regex;
		}

		public String[] getTables() {
			return tables;
		}

		public void setTables(String... tables) {
			support.firePropertyChange("tables", this.tables, tables);
			this.tables = tables;
		}

		public String[] getTablesAny() {
			return tablesAny;
		}

		public void setTablesAny(String... tablesAny) {
			support.firePropertyChange("tablesAny", this.tablesAny, tablesAny);
			this.tablesAny = tablesAny;
		}

		public String[] getTablesAll() {
			return tablesAll;
		}

		public void setTablesAll(String... tablesAll) {
			support.firePropertyChange("tablesAll", this.tablesAll, tablesAll);
			this.tablesAll = tablesAll;
		}

		/**
		 * 
		 * @return Key expiration duration in seconds. Use 0 for no caching, -1 for no
		 *         expiration
		 */
		public long getTtl() {
			return ttl;
		}

		public void setTtl(long ttl) {
			support.firePropertyChange("ttl", this.ttl, ttl);
			this.ttl = ttl;
		}

		@Override
		public String toString() {
			return "RuleConfig [tables=" + tables + ", tablesAny=" + tablesAny + ", tablesAll=" + tablesAll + ", regex="
					+ regex + ", ttl=" + ttl + "]";
		}

		public static Builder tables(String... tables) {
			Builder builder = new Builder();
			builder.tables = tables;
			return builder;
		}

		public static Builder tablesAny(String... tables) {
			Builder builder = new Builder();
			builder.tablesAny = tables;
			return builder;
		}

		public static Builder tablesAll(String... tables) {
			Builder builder = new Builder();
			builder.tablesAll = tables;
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

			private String[] tables;
			private String[] tablesAny;
			private String[] tablesAll;
			private String regex;
			private long ttl = DEFAULT_TTL;

			private Builder() {
			}

			public Builder ttl(long ttl) {
				this.ttl = ttl;
				return this;
			}

			public RuleConfig build() {
				return new RuleConfig(this);
			}
		}

	}

}
