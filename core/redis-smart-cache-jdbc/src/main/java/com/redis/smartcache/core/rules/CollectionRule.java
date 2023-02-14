package com.redis.smartcache.core.rules;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class CollectionRule<T, L, R> extends AbstractRule<L, R> {

	private final Predicate<L> condition;

	public CollectionRule(Function<L, Collection<T>> extractor, Predicate<Collection<T>> predicate,
			Consumer<R> action) {
		this(extractor, predicate, action, Rule.stop());
	}

	public CollectionRule(Function<L, Collection<T>> extractor, Predicate<Collection<T>> predicate, Consumer<R> action,
			Function<L, Control> control) {
		super(action, control);
		this.condition = l -> predicate.test(extractor.apply(l));
	}

	@Override
	public Predicate<L> getCondition() {
		return condition;
	}

	public static <T, L, R> Builder<T, L, R> builder(Function<L, Collection<T>> extractor, Consumer<R> action) {
		return new Builder<>(extractor, action);
	}

	public static class Builder<T, L, R> {

		private final Function<L, Collection<T>> extractor;
		private final Consumer<R> action;
		private Function<L, Control> control = f -> Control.STOP;

		public Builder(Function<L, Collection<T>> extractor, Consumer<R> action) {
			this.extractor = extractor;
			this.action = action;
		}

		public Builder<T, L, R> control(Function<L, Control> control) {
			this.control = control;
			return this;
		}

		public Builder<T, L, R> control(Control control) {
			return control(f -> control);
		}

		@SuppressWarnings("unchecked")
		public CollectionRule<T, L, R> any(T... tableNames) {
			return new CollectionRule<>(extractor, new ContainsAnyPredicate<>(tableNames), action, control);
		}

		@SuppressWarnings("unchecked")
		public CollectionRule<T, L, R> all(T... tableNames) {
			return new CollectionRule<>(extractor, new ContainsAllPredicate<>(tableNames), action, control);
		}

		@SuppressWarnings("unchecked")
		public CollectionRule<T, L, R> exact(T... tableNames) {
			return new CollectionRule<>(extractor, new EqualsPredicate<>(tableNames), action, control);
		}
	}

	public static class EqualsPredicate<T> implements Predicate<Collection<T>> {

		private final Set<T> expected;

		@SuppressWarnings("unchecked")
		public EqualsPredicate(T... expected) {
			this(Arrays.asList(expected));
		}

		public EqualsPredicate(Collection<T> expected) {
			this.expected = new HashSet<>(expected);
		}

		@Override
		public boolean test(Collection<T> t) {
			return expected.equals(t);
		}

	}

	public static class ContainsAnyPredicate<T> implements Predicate<Collection<T>> {

		private final Set<T> expected;

		@SuppressWarnings("unchecked")
		public ContainsAnyPredicate(T... expected) {
			this(Arrays.asList(expected));
		}

		public ContainsAnyPredicate(List<T> expected) {
			this.expected = new HashSet<>(expected);
		}

		@Override
		public boolean test(Collection<T> t) {
			for (T value : t) {
				if (expected.contains(value)) {
					return true;
				}
			}
			return false;
		}

	}

	public static class ContainsAllPredicate<T> implements Predicate<Collection<T>> {

		private final Set<T> expected;

		@SuppressWarnings("unchecked")
		public ContainsAllPredicate(T... expected) {
			this(Arrays.asList(expected));
		}

		public ContainsAllPredicate(Collection<T> expected) {
			this.expected = new HashSet<>(expected);
		}

		@Override
		public boolean test(Collection<T> t) {
			return t.containsAll(expected);
		}

	}

}
