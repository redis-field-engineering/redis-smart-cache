package com.redis.sidecar.rules;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public interface Rule<L, R> {

	public enum Control {
		STOP, CONTINUE
	}

	static <L> Function<L, Control> stop() {
		return l -> Control.STOP;
	}

	Predicate<L> getCondition();

	Consumer<R> getAction();

	Function<L, Control> getControl();

}
