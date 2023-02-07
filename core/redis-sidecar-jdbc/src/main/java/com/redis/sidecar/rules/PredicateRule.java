package com.redis.sidecar.rules;

import java.util.function.Consumer;
import java.util.function.Predicate;

import com.redis.sidecar.SidecarStatement;

public class PredicateRule extends AbstractRule {

	private final Predicate<SidecarStatement> predicate;

	public PredicateRule(Predicate<SidecarStatement> predicate, Consumer<SidecarStatement> action) {
		super(action);
		this.predicate = predicate;
	}

	@Override
	public boolean evaluate(SidecarStatement statement) {
		return predicate.test(statement);
	}

}
