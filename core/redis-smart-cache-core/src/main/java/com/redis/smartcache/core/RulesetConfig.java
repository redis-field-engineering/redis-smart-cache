package com.redis.smartcache.core;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.List;

public class RulesetConfig {
    public static final String PROPERTY_RULES = "rules";

    private final PropertyChangeSupport support = new PropertyChangeSupport(this);
    private List<RuleConfig> rules = new ArrayList<>();

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

    public static RulesetConfig of(RuleConfig... rules) {
        RulesetConfig ruleset = new RulesetConfig();
        for (RuleConfig rule : rules) {
            ruleset.getRules().add(rule);
        }
        return ruleset;
    }
}
