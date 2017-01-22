package part2.cache;

import db.DataStorage;

import java.util.concurrent.*;

public class CachingDataStorageImpl<T> implements CachingDataStorage<String, T> {

    private final DataStorage<String, T> db;
    private final int timeout;
    private final TimeUnit timeoutUnits;
    // TODO can we use Map<String, T> here? Why?
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
        // TODO implement
        // TODO use ScheduledExecutorService to remove outdated result from cache - see SlowCompletableFutureDb implementation
        // TODO complete OutdatableResult::outdated after removing outdated result from cache
        // TODO don't use obtrudeException on result - just don't
        // TODO use remove(Object key, Object value) to remove target value
        // TODO Start timeout after receiving result in CompletableFuture, not after receiving CompletableFuture itself
        CompletableFuture<T> result = new CompletableFuture<>();
        CompletableFuture<Void> outdated = new CompletableFuture<>();
        OutdatableResult<T> outdatableResult = new OutdatableResult<>(result, outdated);
        OutdatableResult<T> cachedResult = cache.putIfAbsent(key, outdatableResult);

        if (cachedResult != null)
            return cachedResult;
        db.get(key)
                .whenComplete((t, thr) -> {
                    if (thr != null) {
                        result.completeExceptionally(thr);
                    } else {
                        result.complete(t);
                    }
                    scheduledExecutorService.schedule(
                            () -> {
                                cache.remove(key, outdatableResult);
                                outdated.complete(null);
                            },
                            timeout,
                            timeoutUnits);
                });
        return outdatableResult;
    }
}
