package com.redis.smartcache.core;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.Arrays;

public class RulesetConfig {

    public static final String PROPERTY_RULES = "rules";
    public static final RuleConfig DEFAULT_RULE = RuleConfig.passthrough().build();

    private final PropertyChangeSupport support = new PropertyChangeSupport(this);
    private RuleConfig[] rules = { DEFAULT_RULE };

    public void addPropertyChangeListener(PropertyChangeListener listener) {
        support.addPropertyChangeListener(listener);
        for (RuleConfig rule : rules) {
            rule.addPropertyChangeListener(listener);
        }
    }

    public void removePropertyChangeListener(PropertyChangeListener listener) {
        support.removePropertyChangeListener(listener);
        for (RuleConfig rule : rules) {
            rule.removePropertyChangeListener(listener);
        }
    }

    public RuleConfig[] getRules() {
        return rules;
    }

    public void setRules(RuleConfig... rules) {
        support.firePropertyChange(PROPERTY_RULES, this.rules, rules);
        this.rules = rules;
    }

    public static RulesetConfig of(RuleConfig... rules) {
        RulesetConfig ruleset = new RulesetConfig();
        ruleset.setRules(rules);
        return ruleset;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(rules);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RulesetConfig other = (RulesetConfig) obj;
        return Arrays.equals(rules, other.rules);
    }

}
