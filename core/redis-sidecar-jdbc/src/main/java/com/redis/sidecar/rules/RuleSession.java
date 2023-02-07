package com.redis.sidecar.rules;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import com.google.common.base.Predicates;
import com.redis.sidecar.RulesConfig;
import com.redis.sidecar.RulesConfig.RuleConfig;
import com.redis.sidecar.SidecarStatement;
import com.redis.sidecar.SqlParser;

public class RuleSession implements PropertyChangeListener {

	private final List<Rule> rules = new ArrayList<>();

	public void fire(SidecarStatement statement) {
		synchronized (rules) {
			for (Rule rule : rules) {
				if (rule.evaluate(statement)) {
					rule.execute(statement);
				}
			}
		}
	}

	private static class TableNameExtractor implements Function<SidecarStatement, Set<String>> {

		private static final SqlParser SQL_PARSER = new SqlParser();

		private Set<String> tableNames;

		@Override
		public Set<String> apply(SidecarStatement statement) {
			if (tableNames == null) {
				tableNames = SQL_PARSER.extractTableNames(statement.getSql());
			}
			return tableNames;
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public void propertyChange(PropertyChangeEvent evt) {
		if (RulesConfig.PROPERTY_RULES.equals(evt.getPropertyName())) {
			updateRules((List<RuleConfig>) evt.getNewValue());
		}
	}

	public void updateRules(List<RuleConfig> ruleConfigs) {
		TableNameExtractor extractor = new TableNameExtractor();
		synchronized (rules) {
			rules.clear();
			for (RuleConfig ruleConfig : ruleConfigs) {
				rules.add(rule(ruleConfig, extractor));
			}
		}
	}

	private Rule rule(RuleConfig rule, TableNameExtractor extractor) {
		Consumer<SidecarStatement> action = action(rule);
		if (rule.getTables() != null) {
			Set<String> tables = new HashSet<>(rule.getTables());
			return new TableRule(tables::equals, extractor, action);
		}
		if (rule.getTablesAll() != null) {
			return new TableRule(new TableRule.ContainsAllPredicate(rule.getTablesAll()), extractor, action);
		}
		if (rule.getTablesAny() != null) {
			return new TableRule(new TableRule.ContainsAnyPredicate(rule.getTablesAll()), extractor, action);
		}
		if (rule.getRegex() != null) {
			return new RegexRule(Pattern.compile(rule.getRegex()), s -> s.getSql(), action);
		}
		return new PredicateRule(Predicates.alwaysTrue(), action);
	}

	private Consumer<SidecarStatement> action(RuleConfig rule) {
		return s -> s.setTtl(rule.getTtl());
	}

}
