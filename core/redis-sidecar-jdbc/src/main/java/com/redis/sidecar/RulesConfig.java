package com.redis.sidecar;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import io.lettuce.core.internal.LettuceAssert;

public class RulesConfig {

	public static final String PROPERTY_RULES = "rules";

	private List<RuleConfig> rules = Arrays.asList(RuleConfig.builder().build());

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
		support.firePropertyChange(PROPERTY_RULES, this.rules, rules);
		this.rules = rules;
	}

	public static class RuleConfig {

		public static final long TTL_NO_CACHE = 0;
		public static final long TTL_NO_EXPIRATION = -1;
		public static final Duration DEFAULT_TTL = Duration.ofHours(1);

		private final PropertyChangeSupport support = new PropertyChangeSupport(this);

		private List<String> tables;
		private List<String> tablesAny;
		private List<String> tablesAll;
		private String regex;
		private long ttl = DEFAULT_TTL.toSeconds();

		public RuleConfig() {
		}

		private RuleConfig(Builder builder) {
			this.tables = builder.tables;
			this.tablesAny = builder.tablesAny;
			this.tablesAll = builder.tablesAll;
			this.regex = builder.regex;
			this.ttl = builder.ttl.toSeconds();
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

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private List<String> tables;
			private List<String> tablesAny;
			private List<String> tablesAll;
			private String regex;
			private Duration ttl = DEFAULT_TTL;

			private Builder() {
			}

			public Builder tables(List<String> tables) {
				this.tables = tables;
				return this;
			}

			public Builder tablesAny(List<String> tablesAny) {
				this.tablesAny = tablesAny;
				return this;
			}

			public Builder tablesAll(List<String> tablesAll) {
				this.tablesAll = tablesAll;
				return this;
			}

			public Builder regex(String regex) {
				this.regex = regex;
				return this;
			}

			public Builder ttl(Duration ttl) {
				LettuceAssert.notNull(ttl, "TTL must not be null");
				LettuceAssert.isTrue(!ttl.isNegative(), "TTL must be zero or greater");
				this.ttl = ttl;
				return this;
			}

			public RuleConfig build() {
				return new RuleConfig(this);
			}
		}

	}

}
