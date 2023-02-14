package com.redis.smartcache.core.rules;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class RegexRule<L, R> extends AbstractRule<L, R> {

	private final Predicate<L> condition;

	public RegexRule(Pattern pattern, Function<L, String> stringExtractor, Consumer<R> action) {
		this(pattern, stringExtractor, action, Rule.stop());
	}

	public RegexRule(Pattern pattern, Function<L, String> stringExtractor, Consumer<R> action,
			Function<L, Control> control) {
		super(action, control);
		this.condition = f -> pattern.matcher(stringExtractor.apply(f)).matches();
	}

	@Override
	public Predicate<L> getCondition() {
		return condition;
	}

}
