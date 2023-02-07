package com.redis.sidecar.rules;

import java.util.function.Consumer;

import com.redis.sidecar.SidecarStatement;

public abstract class AbstractRule implements Rule {

	private final Consumer<SidecarStatement> action;

	protected AbstractRule(Consumer<SidecarStatement> statementConsumer) {
		this.action = statementConsumer;
	}

	@Override
	public void execute(SidecarStatement statement) {
		this.action.accept(statement);
	}

}
