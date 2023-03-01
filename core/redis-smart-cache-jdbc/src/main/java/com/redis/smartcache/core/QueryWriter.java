package com.redis.smartcache.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.smartcache.core.Config.AnalyzerConfig;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.internal.Futures;
import io.lettuce.core.support.ConnectionPoolSupport;

public class QueryWriter implements AutoCloseable {

	public static final String FIELD_ID = "id";
	public static final String FIELD_TABLE = "table";
	public static final String FIELD_SQL = "sql";
	private static final long POLL_TIMEOUT = 100;

	private static final Logger log = Logger.getLogger(QueryWriter.class.getName());

	private final AbstractRedisClient client;
	private final GenericObjectPool<StatefulRedisModulesConnection<String, String>> pool;
	private final ExecutorService executor;
	private final BlockingQueue<Query> queue;
	private boolean stopped;
	private List<Future<?>> futures = new ArrayList<>();
	private final KeyBuilder keyBuilder;
	private final AnalyzerConfig config;
	private final String index;
	private final Map<String, Query> queryCache;

	public QueryWriter(AbstractRedisClient client, AnalyzerConfig config, String index, KeyBuilder keyBuilder) {
		this.client = client;
		this.config = config;
		this.pool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(), poolConfig(config));
		this.executor = Executors.newFixedThreadPool(config.getThreads());
		this.queue = new LinkedBlockingDeque<>(config.getQueueCapacity());
		this.index = index;
		this.keyBuilder = keyBuilder;
		createIndex();
		IntStream.range(0, config.getThreads()).mapToObj(Task::new).map(executor::submit).forEach(futures::add);
		queryCache = new EvictingLinkedHashMap<>(config.getCacheCapacity());
	}

	private GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> poolConfig(AnalyzerConfig config) {
		GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(config.getPoolSize());
		return poolConfig;
	}

	private Supplier<StatefulRedisModulesConnection<String, String>> connectionSupplier() {
		if (client instanceof RedisModulesClusterClient) {
			return ((RedisModulesClusterClient) client)::connect;
		}
		return ((RedisModulesClient) client)::connect;
	}

	@SuppressWarnings("unchecked")
	private void createIndex() {
		try (StatefulRedisModulesConnection<String, String> conn = pool.borrowObject()) {
			if (RedisModulesUtils.indexInfo(() -> conn.sync().ftInfo(index)).isPresent()) {
				return;
			}
			CreateOptions.Builder<String, String> options = CreateOptions.builder();
			options.prefix(keyBuilder.getKeyspace() + keyBuilder.getSeparator());
			conn.sync().ftCreate(index, options.build(), Field.<String>tag(FIELD_ID).build(),
					Field.<String>tag(FIELD_TABLE).build(), Field.<String>text(FIELD_SQL).noStem().build());
		} catch (Exception e) {
			log.log(Level.SEVERE, "Could not create index", e);
		}
	}

	public void write(Query query) {
		if (stopped) {
			throw new IllegalStateException("RediSearchQueryWriter not started");
		}
		if (!queue.add(query)) {
			log.warning("Queue is full");
		}

	}

	private class Task implements Runnable {

		private final int taskId;

		public Task(int id) {
			this.taskId = id;
		}

		@Override
		public void run() {
			long lastFlush = System.currentTimeMillis();
			List<Query> queries = new ArrayList<>();
			while (!stopped) {
				Query query;
				try {
					query = queue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					log.fine("Interrupted");
					Thread.currentThread().interrupt();
					return;
				}
				if (query != null && !queries.add(query)) {
					log.log(Level.WARNING, "Task {0} could not add query {1}", new Object[] { taskId, query.getId() });
				}
				if (queries.size() >= config.getBatchSize()
						|| System.currentTimeMillis() - lastFlush > config.getFlushInterval().toMillis()) {
					log.log(Level.FINE, "Writing {0} queries", queries.size());
					writeBatch(queries);
					queries.clear();
					lastFlush = System.currentTimeMillis();
				}
			}
		}

		private boolean writeBatch(List<Query> queries) {
			List<RedisFuture<Long>> redisFutures = new ArrayList<>();
			if (pool.isClosed()) {
				return false;
			}
			try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
				connection.setAutoFlushCommands(false);
				try {
					RedisModulesAsyncCommands<String, String> commands = connection.async();
					for (Query query : queries) {
						Map<String, String> hash = new HashMap<>();
						hash.put(FIELD_ID, query.getId());
						hash.put(FIELD_TABLE, query.getTables().stream().collect(Collectors.joining(",")));
						hash.put(FIELD_SQL, query.getSql());
						redisFutures.add(commands.hset(keyBuilder.create(query.getId()), hash));
					}
					connection.flushCommands();
					LettuceFutures.awaitAll(connection.getTimeout(), redisFutures.toArray(new RedisFuture[0]));
					return true;
				} finally {
					connection.setAutoFlushCommands(true);
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not get connection from pool", e);
				return false;
			}
		}
	}

	@Override
	public void close() {
		this.stopped = true;
		queue.clear();
		Futures.awaitAll(Duration.ofSeconds(10), futures.toArray(new Future[0]));
		executor.shutdown();
		pool.close();
		queryCache.clear();
	}

	public Query computeIfAbsent(String sql, Function<String, Query> mappingFunction) {
		return queryCache.computeIfAbsent(sql, s -> {
			Query query = mappingFunction.apply(s);
			write(query);
			return query;
		});
	}

}
