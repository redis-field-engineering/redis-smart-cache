package com.redis.smartcache.core.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.redis.smartcache.core.rules.Rule.Control;

public class RuleSession<L, R> {

	private final List<Rule<L, R>> rules;

	public RuleSession() {
		this(new ArrayList<>());
	}

	public RuleSession(Collection<Rule<L, R>> rules) {
		this.rules = new ArrayList<>(rules);
	}

	public List<Rule<L, R>> getRules() {
		return rules;
	}

	public void fire(L fact, R action) {
		synchronized (rules) {
			for (Rule<L, R> rule : rules) {
				if (rule.getCondition().test(fact)) {
					rule.getAction().accept(action);
					if (rule.getControl().apply(fact) == Control.STOP) {
						return;
					}
				}
			}
		}
	}

	public void setRules(List<Rule<L, R>> rules) {
		synchronized (this.rules) {
			this.rules.clear();
			this.rules.addAll(rules);
		}
	}

}
