package com.redis.smartcache;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Predicates;
import com.redis.smartcache.RulesetConfig.RuleConfig;
import com.redis.smartcache.rules.CollectionRule;
import com.redis.smartcache.rules.PredicateRule;
import com.redis.smartcache.rules.RegexRule;
import com.redis.smartcache.rules.Rule;
import com.redis.smartcache.rules.RuleSession;

public class StatementRuleSession extends RuleSession<SmartCacheStatement, SmartCacheStatement>
		implements PropertyChangeListener {

	public StatementRuleSession() {
		this(new ArrayList<>());
	}

	public StatementRuleSession(List<Rule<SmartCacheStatement, SmartCacheStatement>> rules) {
		super(rules);
	}

	public static StatementRuleSession of(RulesetConfig ruleset) {
		return new StatementRuleSession(rules(ruleset.getRules().toArray(RuleConfig[]::new)));
	}

	public void fire(SmartCacheStatement statement) {
		super.fire(statement, statement);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void propertyChange(PropertyChangeEvent evt) {
		if (RulesetConfig.PROPERTY_RULESET.equals(evt.getPropertyName())) {
			updateRules((List<RuleConfig>) evt.getNewValue());
		}
	}

	public void updateRules(List<RuleConfig> ruleConfigs) {
		synchronized (rules) {
			rules.clear();
			for (RuleConfig ruleConfig : ruleConfigs) {
				rules.add(rule(ruleConfig));
			}
		}
	}

	private static List<Rule<SmartCacheStatement, SmartCacheStatement>> rules(RuleConfig... rules) {
		return Stream.of(rules).map(StatementRuleSession::rule).collect(Collectors.toList());
	}

	private static Rule<SmartCacheStatement, SmartCacheStatement> rule(RuleConfig rule) {
		Consumer<SmartCacheStatement> action = action(rule);
		if (rule.getTables() != null) {
			return CollectionRule.builder(SmartCacheStatement::getTableNames, action).exact(rule.getTables());
		}
		if (rule.getTablesAll() != null) {
			return CollectionRule.builder(SmartCacheStatement::getTableNames, action).all(rule.getTablesAll());
		}
		if (rule.getTablesAny() != null) {
			return CollectionRule.builder(SmartCacheStatement::getTableNames, action).any(rule.getTablesAny());
		}
		if (rule.getRegex() != null) {
			return new RegexRule<>(Pattern.compile(rule.getRegex()), SmartCacheStatement::getSql, action);
		}
		return new PredicateRule<>(Predicates.alwaysTrue(), action);
	}

	private static Consumer<SmartCacheStatement> action(RuleConfig rule) {
		return s -> s.setTtl(rule.getTtl());
	}

}
