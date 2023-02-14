package com.redis.smartcache.core;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.redis.smartcache.jdbc.SmartConnection;

import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;

public class Query {

	public static final long TTL_NO_CACHING = 0;
	public static final long TTL_NO_EXPIRATION = -1;

	private final String key;
	private final String sql;
	private final Statement statement;
	private long ttl = TTL_NO_CACHING;

	public Query(String key, String sql, Statement statement) {
		this.key = key;
		this.sql = sql;
		this.statement = statement;
	}

	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	public boolean isCaching() {
		return ttl != TTL_NO_CACHING;
	}

	public String getKey() {
		return key;
	}

	public String getSql() {
		return sql;
	}

	public Stream<Table> getTables() throws ParsingException {
		if (statement == null) {
			return Stream.empty();
		}
		return statement.accept(DepthFirstVisitor.by(new TableVisitor()), null);
	}

	public Set<String> getTableNames() {
		return getTables().map(Table::getName).map(QualifiedName::toString).collect(Collectors.toSet());
	}

	public static Query of(String key, String sql) {
		SqlParser parser = new SqlParser();
		return new Query(key, sql, parser.createStatement(sql, new ParsingOptions()));
	}

	public static Query of(String sql) {
		return of(SmartConnection.crc32(sql), sql);
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
