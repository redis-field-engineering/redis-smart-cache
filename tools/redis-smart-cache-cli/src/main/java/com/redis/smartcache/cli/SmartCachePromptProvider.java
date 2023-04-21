package com.redis.smartcache.cli;

import org.jline.utils.AttributedString;
import org.springframework.shell.jline.PromptProvider;
import org.springframework.stereotype.Component;

@Component
public class SmartCachePromptProvider implements PromptProvider {

	@Override
	public AttributedString getPrompt() {
		return new AttributedString("smart-cache:>");
	}

}