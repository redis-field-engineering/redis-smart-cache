package com.redis.smartcache.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.function.Function;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;

import com.redis.smartcache.jdbc.rowset.CachedRowSetFactory;

import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public class RedisResultSetCache implements ResultSetCache {

	private static final String METER_PREFIX_CACHE = "cache";
	private static final String METER_QUERY = "query";
	private static final String METER_BACKEND = "backend";
	private static final String METER_CACHE_GET = METER_PREFIX_CACHE + ".get";
	private static final String METER_CACHE_PUT = METER_PREFIX_CACHE + ".put";
	private static final String TAG_RESULT = "result";
	private static final String TAG_MISS = "miss";
	private static final String TAG_HIT = "hit";
	private static final String TAG_QUERY = "query";

	private final SQLParser parser = new SQLParser();
	private final RowSetFactory rowSetFactory = new CachedRowSetFactory();
	private final StatefulRedisConnection<String, ResultSet> connection;
	private final MeterRegistry meterRegistry;
	private final QueryRuleSession ruleSession;
	private final KeyBuilder keyBuilder;
	private final QueryWriter queryWriter;

	public RedisResultSetCache(StatefulRedisConnection<String, ResultSet> connection, MeterRegistry meterRegistry,
			KeyBuilder keyBuilder, QueryRuleSession ruleSession, QueryWriter queryWriter) {
		this.connection = connection;
		this.meterRegistry = meterRegistry;
		this.keyBuilder = keyBuilder;
		this.ruleSession = ruleSession;
		this.queryWriter = queryWriter;
	}

	private Query getQuery(String sql) {
		synchronized (queryWriter) {
			return queryWriter.computeIfAbsent(sql, this::newQuery);
		}
	}

	private Query newQuery(String sql) {
		Query query = new Query();
		query.setId(hash(sql));
		query.setSql(sql);
		query.setTables(parser.extractTableNames(sql));
		createMeters(query);
		return query;
	}

	private String hash(String sql) {
		return String.valueOf(HashingFunctions.crc32(sql));
	}

	@Override
	public CachedResultSet get(String sql) {
		return get(sql, this::key);
	}

	@Override
	public CachedResultSet get(String sql, Collection<String> parameters) {
		return get(sql, q -> key(q, parameters));
	}

	private CachedResultSet get(String sql, Function<Query, String> keyMappingFunction) {
		CachedResultSet cachedResultSet = cachedResultSet(sql, keyMappingFunction);
		populateFromCache(cachedResultSet);
		return cachedResultSet;
	}

	private void populateFromCache(CachedResultSet cachedResultSet) {
		if (cachedResultSet.getAction().isCaching()) {
			Timer timer = getTimer(METER_CACHE_GET, cachedResultSet.getQuery());
			ResultSet resultSet = timer.record(() -> connection.sync().get(cachedResultSet.getKey()));
			cachedResultSet.setResultSet(resultSet);
			meterRegistry.get(METER_CACHE_GET).tags(tags(cachedResultSet.getQuery()))
					.tags(TAG_RESULT, cachedResultSet.hasResultSet() ? TAG_HIT : TAG_MISS).counter().increment();
		}
	}

	private String key(Query query) {
		return keyBuilder.create(query.getId());
	}

	private String key(Query query, Collection<String> parameters) {
		return keyBuilder.create(query.getId(), hash(String.join(keyBuilder.getSeparator(), parameters)));
	}

	private CachedResultSet cachedResultSet(String sql, Function<Query, String> keyMappingFunction) {
		Query query = getQuery(sql);
		String key = keyMappingFunction.apply(query);
		Action action = ruleSession.fire(query);
		return new CachedResultSet(key, query, action);
	}

	private CachedResultSet put(CachedResultSet cachedResultSet, Executable<ResultSet> executable) throws SQLException {
		Timer backendTimer = getTimer(METER_BACKEND, cachedResultSet.getQuery());
		Timer cachePutTimer = getTimer(METER_CACHE_PUT, cachedResultSet.getQuery());
		try {
			ResultSet resultSet = backendTimer.recordCallable(executable);
			return cachePutTimer.recordCallable(() -> put(cachedResultSet, resultSet));
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	private CachedResultSet put(CachedResultSet cachedResultSet, ResultSet resultSet) throws SQLException {
		CachedRowSet cachedRowSet = rowSetFactory.createCachedRowSet();
		cachedRowSet.populate(resultSet);
		cachedRowSet.beforeFirst();
		SetArgs args = SetArgs.Builder.ex(cachedResultSet.getAction().getTtl());
		connection.sync().set(cachedResultSet.getKey(), cachedRowSet, args);
		cachedRowSet.beforeFirst();
		cachedResultSet.setResultSet(cachedRowSet);
		return cachedResultSet;
	}

	@Override
	public void close() {
		connection.close();
	}

	@Override
	public CachedResultSet computeIfAbsent(String sql, Executable<ResultSet> executable) throws SQLException {
		return computeIfAbsent(sql, this::key, executable);
	}

	@Override
	public CachedResultSet computeIfAbsent(String sql, Collection<String> parameters, Executable<ResultSet> executable)
			throws SQLException {
		return computeIfAbsent(sql, q -> key(q, parameters), executable);
	}

	private CachedResultSet computeIfAbsent(String sql, Function<Query, String> mapper, Executable<ResultSet> backend)
			throws SQLException {
		CachedResultSet cachedResultSet = cachedResultSet(sql, mapper);
		try {
			return queryTimer(cachedResultSet).recordCallable(() -> {
				populateFromCache(cachedResultSet);
				if (!cachedResultSet.hasResultSet()) {
					put(cachedResultSet, backend);
				}
				return cachedResultSet;
			});
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	private Timer queryTimer(CachedResultSet cachedResultSet) {
		return getTimer(METER_QUERY, cachedResultSet.getQuery());
	}

	@Override
	public CachedResultSet get(CachedResultSet cachedResultSet, Executable<ResultSet> executable) throws SQLException {
		if (!cachedResultSet.hasResultSet()) {
			put(cachedResultSet, executable);
		}
		return cachedResultSet;
	}

	private Timer getTimer(String name, Query query) {
		return meterRegistry.get(name).tags(tags(query)).timer();
	}

	private Timer createTimer(String name, Query query) {
		return Timer.builder(name).tags(tags(query)).publishPercentiles(0.9, 0.99).register(meterRegistry);
	}

	private Tags tags(Query query) {
		return Tags.of(TAG_QUERY, query.getId());
	}

	private Counter createCounter(String name, Query query, String... tags) {
		return Counter.builder(name).tags(tags(query)).tags(tags).register(meterRegistry);
	}

	public void createMeters(Query query) {
		createTimer(METER_QUERY, query);
		createTimer(METER_BACKEND, query);
		createTimer(METER_CACHE_GET, query);
		createTimer(METER_CACHE_PUT, query);
		createCounter(METER_CACHE_GET, query, TAG_RESULT, TAG_HIT);
		createCounter(METER_CACHE_GET, query, TAG_RESULT, TAG_MISS);
	}

}
