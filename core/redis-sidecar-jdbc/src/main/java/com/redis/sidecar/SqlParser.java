package com.redis.sidecar;

import java.util.Objects;
import java.util.stream.Stream;

import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Table;

public class SqlParser {

	private final io.trino.sql.parser.SqlParser parser = new io.trino.sql.parser.SqlParser();
	private final ParsingOptions options = new ParsingOptions();

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

	public static AstVisitor<Table, Object> extractTables() {
		return new AstVisitor<Table, Object>() {
			@Override
			protected Table visitTable(Table node, Object context) {
				return node;
			}
		};
	}

	public Stream<Table> getTables(String sql) throws ParsingException {
		return parser.createStatement(sql, options).accept(DepthFirstVisitor.by(extractTables()), null);
	}

}
