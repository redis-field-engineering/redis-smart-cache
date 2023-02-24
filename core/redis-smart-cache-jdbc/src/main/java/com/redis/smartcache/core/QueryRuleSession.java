package com.redis.smartcache.core;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;
import com.redis.smartcache.core.Config.RulesetConfig;
import com.redis.smartcache.core.Config.RulesetConfig.RuleConfig;
import com.redis.smartcache.core.rules.CollectionRule;
import com.redis.smartcache.core.rules.PredicateRule;
import com.redis.smartcache.core.rules.RegexRule;
import com.redis.smartcache.core.rules.Rule;
import com.redis.smartcache.core.rules.RuleSession;

public class QueryRuleSession extends RuleSession<Query, Query> implements PropertyChangeListener {

	public QueryRuleSession() {
		super();
	}

	public QueryRuleSession(List<Rule<Query, Query>> rules) {
		super(rules);
	}

	public static QueryRuleSession of(RulesetConfig ruleset) {
		return new QueryRuleSession(rules(ruleset));
	}

	private static List<Rule<Query, Query>> rules(RulesetConfig ruleset) {
		return ruleset.getRules().stream().map(QueryRuleSession::rule).collect(Collectors.toList());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void propertyChange(PropertyChangeEvent evt) {
		if (RulesetConfig.PROPERTY_RULES.equals(evt.getPropertyName())) {
			updateRules((List<RuleConfig>) evt.getNewValue());
		}
	}

	public void updateRules(List<RuleConfig> ruleConfigs) {
		setRules(ruleConfigs.stream().map(QueryRuleSession::rule).collect(Collectors.toList()));
	}

	public void fire(Query query) {
		super.fire(query, query);
	}

	private static Rule<Query, Query> rule(RuleConfig rule) {
		Consumer<Query> action = action(rule);
		if (rule.getTables() != null) {
			return CollectionRule.builder(Query::getTables, action).exact(rule.getTables());
		}
		if (rule.getTablesAll() != null) {
			return CollectionRule.builder(Query::getTables, action).all(rule.getTablesAll());
		}
		if (rule.getTablesAny() != null) {
			return CollectionRule.builder(Query::getTables, action).any(rule.getTablesAny());
		}
		if (rule.getRegex() != null) {
			return new RegexRule<>(Pattern.compile(rule.getRegex()), Query::getSql, action);
		}
		return new PredicateRule<>(Predicates.alwaysTrue(), action);
	}

	private static Consumer<Query> action(RuleConfig rule) {
		return s -> s.setTtl(rule.getTtl());
	}

}
