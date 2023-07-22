package com.redis.smartcache.core;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.redis.smartcache.core.config.RulesetConfig;
import com.redis.smartcache.core.rules.CollectionRule;
import com.redis.smartcache.core.rules.PredicateRule;
import com.redis.smartcache.core.rules.RegexRule;
import com.redis.smartcache.core.rules.Rule;
import com.redis.smartcache.core.rules.RuleSession;

public class QueryRuleSession extends RuleSession<Query, Action> implements PropertyChangeListener {

    private static final Logger log = Logger.getLogger(QueryRuleSession.class.getName());

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

    @Override
    public void propertyChange(PropertyChangeEvent evt) {
        if (RulesetConfig.PROPERTY_RULES.equals(evt.getPropertyName())) {
            updateRules((RuleConfig[]) evt.getNewValue());
        }
    }

    public void updateRules(RuleConfig[] ruleConfigs) {
        setRules(Stream.of(ruleConfigs).map(QueryRuleSession::rule).collect(Collectors.toList()));
        log.log(Level.INFO, "Updated rules: {0}", Arrays.toString(ruleConfigs));
    }

    public Action fire(Query query) {
        Action action = new Action();
        fire(query, action);
        return action;
    }

    private static Rule<Query, Action> rule(RuleConfig rule) {
        Consumer<Action> action = a -> a.setTtl(rule.getTtl().toMillis());
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
        return new PredicateRule<>(l -> true, action);
    }

}
