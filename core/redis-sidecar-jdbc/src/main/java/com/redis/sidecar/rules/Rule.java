package com.redis.sidecar.rules;

import com.redis.sidecar.SidecarStatement;

public interface Rule {

	/**
	 * 
	 * @param statement the statement needed for evaluating rule conditions
	 * @return true if rule should be executed
	 */
	boolean evaluate(SidecarStatement statement);

	/**
	 * 
	 * @param statement executes the rule by modifying the given statement
	 */
	void execute(SidecarStatement statement);

}
