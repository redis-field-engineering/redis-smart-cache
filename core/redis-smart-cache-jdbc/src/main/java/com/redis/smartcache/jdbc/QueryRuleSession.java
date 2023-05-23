package com.redis.smartcache.jdbc;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Predicates;
import com.redis.smartcache.core.RuleConfig;
import com.redis.smartcache.core.RulesetConfig;
import com.redis.smartcache.core.Query;
import com.redis.smartcache.core.rules.CollectionRule;
import com.redis.smartcache.core.rules.PredicateRule;
import com.redis.smartcache.core.rules.RegexRule;
import com.redis.smartcache.core.rules.Rule;
import com.redis.smartcache.core.rules.RuleSession;

public class QueryRuleSession extends RuleSession<Query, Action> implements PropertyChangeListener {

	public QueryRuleSession() {
		super();
	}

	public QueryRuleSession(List<Rule<Query, Action>> rules) {
		super(rules);
	}

	public static QueryRuleSession of(RulesetConfig ruleset) {
		return new QueryRuleSession(rules(ruleset));
	}

	private static List<Rule<Query, Action>> rules(RulesetConfig ruleset) {
		return Stream.of(ruleset.getRules()).map(QueryRuleSession::rule).collect(Collectors.toList());
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

	public Action fire(Query query) {
		Action action = new Action();
		fire(query, action);
		return action;
	}

	private static Rule<Query, Action> rule(RuleConfig rule) {
		Consumer<Action> action = action(rule);
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
		if (rule.getQueryIds() != null) {
			return new PredicateRule<>(q -> rule.getQueryIds().contains(q.getId()), action);
		}
		return new PredicateRule<>(Predicates.alwaysTrue(), action);
	}

	private static Consumer<Action> action(RuleConfig rule) {
		return a -> a.setTtl(Duration.ofMillis(rule.getTtl().toMillis()));
	}

}
