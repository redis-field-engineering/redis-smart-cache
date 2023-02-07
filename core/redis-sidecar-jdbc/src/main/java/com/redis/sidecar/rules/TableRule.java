package com.redis.sidecar.rules;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.redis.sidecar.SidecarStatement;

public class TableRule extends AbstractRule {

	private final Function<SidecarStatement, Set<String>> tableNamesExtractor;
	private final Predicate<Set<String>> predicate;

	public TableRule(Predicate<Set<String>> predicate, Function<SidecarStatement, Set<String>> tableNamesExtractor,
			Consumer<SidecarStatement> action) {
		super(action);
		this.tableNamesExtractor = tableNamesExtractor;
		this.predicate = predicate;
	}

	@Override
	public boolean evaluate(SidecarStatement facts) {
		return predicate.test(tableNamesExtractor.apply(facts));
	}

	public static class ContainsAnyPredicate implements Predicate<Set<String>> {

		private final Set<String> expected;

		public ContainsAnyPredicate(String... expected) {
			this(Arrays.asList(expected));
		}

		public ContainsAnyPredicate(List<String> expected) {
			this.expected = new HashSet<>(expected);
		}

		@Override
		public boolean test(Set<String> t) {
			for (String value : t) {
				if (expected.contains(value)) {
					return true;
				}
			}
			return false;
		}

	}

	public static class ContainsAllPredicate implements Predicate<Set<String>> {

		private final Set<String> expected;

		public ContainsAllPredicate(String... expected) {
			this(Arrays.asList(expected));
		}

		public ContainsAllPredicate(List<String> expected) {
			this.expected = new HashSet<>(expected);
		}

		@Override
		public boolean test(Set<String> t) {
			return t.containsAll(expected);
		}

	}

}
