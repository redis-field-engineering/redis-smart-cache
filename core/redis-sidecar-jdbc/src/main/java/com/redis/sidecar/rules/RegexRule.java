package com.redis.sidecar.rules;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import com.redis.sidecar.SidecarStatement;

public class RegexRule extends AbstractRule {

	private final Function<SidecarStatement, String> stringExtractor;
	private final Pattern pattern;

	public RegexRule(Pattern pattern, Function<SidecarStatement, String> stringExtractor,
			Consumer<SidecarStatement> action) {
		super(action);
		this.stringExtractor = stringExtractor;
		this.pattern = pattern;
	}

	public Pattern getPattern() {
		return pattern;
	}

	@Override
	public boolean evaluate(SidecarStatement facts) {
		return pattern.matcher(stringExtractor.apply(facts)).matches();
	}

}
