package com.redis.sidecar;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Predicates;
import com.redis.sidecar.RulesetConfig.RuleConfig;
import com.redis.sidecar.rules.CollectionRule;
import com.redis.sidecar.rules.PredicateRule;
import com.redis.sidecar.rules.RegexRule;
import com.redis.sidecar.rules.Rule;
import com.redis.sidecar.rules.RuleSession;

public class StatementRuleSession extends RuleSession<SidecarStatement, SidecarStatement>
		implements PropertyChangeListener {

	public StatementRuleSession() {
		this(new ArrayList<>());
	}

	public StatementRuleSession(List<Rule<SidecarStatement, SidecarStatement>> rules) {
		super(rules);
	}

	public static StatementRuleSession of(RulesetConfig ruleset) {
		return new StatementRuleSession(rules(ruleset.getRules().toArray(RuleConfig[]::new)));
	}

	public void fire(SidecarStatement statement) {
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

	private static List<Rule<SidecarStatement, SidecarStatement>> rules(RuleConfig... rules) {
		return Stream.of(rules).map(StatementRuleSession::rule).collect(Collectors.toList());
	}

	private static Rule<SidecarStatement, SidecarStatement> rule(RuleConfig rule) {
		Consumer<SidecarStatement> action = action(rule);
		if (rule.getTables() != null) {
			return CollectionRule.builder(SidecarStatement::getTableNames, action).exact(rule.getTables());
		}
		if (rule.getTablesAll() != null) {
			return CollectionRule.builder(SidecarStatement::getTableNames, action).all(rule.getTablesAll());
		}
		if (rule.getTablesAny() != null) {
			return CollectionRule.builder(SidecarStatement::getTableNames, action).any(rule.getTablesAny());
		}
		if (rule.getRegex() != null) {
			return new RegexRule<>(Pattern.compile(rule.getRegex()), SidecarStatement::getSql, action);
		}
		return new PredicateRule<>(Predicates.alwaysTrue(), action);
	}

	private static Consumer<SidecarStatement> action(RuleConfig rule) {
		return s -> s.setTtl(rule.getTtl());
	}

}
