package com.redis.smartcache.core;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;

public class SQLParser {

	private static final ParsingOptions PARSING_OPTIONS = new ParsingOptions();
	private static final Set<String> EMPTY_TABLE_NAMES = Collections.emptySet();
	private final SqlParser parser = new SqlParser();

	public Set<String> extractTableNames(String sql) {
		try {
			Statement statement = parser.createStatement(sql, PARSING_OPTIONS);
			Stream<Table> tables = tables(statement);
			return tableNames(tables);
		} catch (ParsingException e) {
			// This statement cannot be parsed. Only rules like regex will trigger
			return EMPTY_TABLE_NAMES;
		}
	}

	private Stream<Table> tables(Statement statement) throws ParsingException {
		return statement.accept(DepthFirstVisitor.by(new TableVisitor()), null);
	}

	private Set<String> tableNames(Stream<Table> tables) {
		return tables.map(Table::getName).map(QualifiedName::toString).collect(Collectors.toSet());
	}

	static class DepthFirstVisitor<R, C> extends AstVisitor<Stream<R>, C> {

		private final AstVisitor<R, C> visitor;

		public DepthFirstVisitor(AstVisitor<R, C> visitor) {
			this.visitor = visitor;
		}

		public static <R, C> DepthFirstVisitor<R, C> by(AstVisitor<R, C> visitor) {
			return new DepthFirstVisitor<>(visitor);
		}

		@Override
		public final Stream<R> visitNode(Node node, C context) {
			Stream<R> nodeResult = Stream.of(visitor.process(node, context));
			Stream<R> childrenResult = node.getChildren().stream().flatMap(child -> process(child, context));

			return Stream.concat(nodeResult, childrenResult).filter(Objects::nonNull);
		}
	}

	static class TableVisitor extends AstVisitor<Table, Object> {

		@Override
		protected Table visitTable(Table node, Object context) {
			return node;
		}
	}

}
