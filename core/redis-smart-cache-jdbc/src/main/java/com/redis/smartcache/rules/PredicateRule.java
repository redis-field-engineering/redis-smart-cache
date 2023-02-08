package com.redis.smartcache.rules;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class PredicateRule<L, R> extends AbstractRule<L, R> {

	private final Predicate<L> predicate;

	public PredicateRule(Predicate<L> predicate, Consumer<R> action) {
		this(predicate, action, Rule.stop());
	}

	public PredicateRule(Predicate<L> predicate, Consumer<R> action, Function<L, Control> control) {
		super(action, control);
		this.predicate = predicate;
	}

	@Override
	public Predicate<L> getCondition() {
		return predicate;
	}

}
