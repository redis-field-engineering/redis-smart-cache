package com.redis.smartcache.core.rules;

import java.util.function.Consumer;
import java.util.function.Function;

public abstract class AbstractRule<L, R> implements Rule<L, R> {

	private final Consumer<R> action;
	private final Function<L, Control> control;

	protected AbstractRule(Consumer<R> action) {
		this(action, l -> Control.STOP);
	}

	protected AbstractRule(Consumer<R> action, Function<L, Control> control) {
		this.action = action;
		this.control = control;
	}

	@Override
	public Consumer<R> getAction() {
		return action;
	}

	@Override
	public Function<L, Control> getControl() {
		return control;
	}

}
