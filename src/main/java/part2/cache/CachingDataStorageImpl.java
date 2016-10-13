package part2.cache;

import db.DataStorage;

import java.util.concurrent.*;

public class CachingDataStorageImpl<T> implements CachingDataStorage<String, T> {

    private final DataStorage<String, T> db;
    private final int timeout;
    private final TimeUnit timeoutUnits;

    private final ConcurrentMap<String, OutdatableResult<T>> cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                private final ThreadFactory threadFactory = Executors.defaultThreadFactory();

                @Override
                public Thread newThread(Runnable r) {
                    final Thread thread = threadFactory.newThread(r);
                    thread.setDaemon(true);
                    return thread;
                }
            });

    public CachingDataStorageImpl(DataStorage<String, T> db, int timeout, TimeUnit timeoutUnits) {
        this.db = db;
        this.timeout = timeout;
        this.timeoutUnits = timeoutUnits;
    }

    @Override
    public OutdatableResult<T> getOutdatable(String key) {
        final CompletableFuture<Void> outdated = new CompletableFuture<>();
        final CompletableFuture<T> dbResponse = new CompletableFuture<>();
        final OutdatableResult<T> result = new OutdatableResult<>(dbResponse, outdated);
        final OutdatableResult<T> cachedResult = cache.putIfAbsent(key, result);
        if (cachedResult != null) return cachedResult;

        db.get(key).thenAccept(dbResponse::complete);
        dbResponse.thenRun(() -> scheduledExecutorService.schedule(
                () -> {
                    cache.remove(key, result);
                    outdated.complete(null);
                },
                timeout,
                timeoutUnits));

        return result;
    }
}
