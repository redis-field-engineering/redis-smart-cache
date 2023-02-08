package com.redis.sidecar.rules;

import java.util.List;

import com.redis.sidecar.rules.Rule.Control;

public class RuleSession<L, R> {

	protected final List<Rule<L, R>> rules;

	public RuleSession(List<Rule<L, R>> rules) {
		this.rules = rules;
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

}
