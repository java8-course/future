package part2.cache;

import db.DataStorage;
import db.SlowCompletableFutureDb;

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
        //  implement
        // use ScheduledExecutorService to remove outdated result from cache
        // - see SlowCompletableFutureDb implementation
        // complete OutdatableResult::outdated after removing outdated result from cache
        // don't use obtrudeException on result - just don't
        // use remove(Object key, Object value) to remove target value
        // Start timeout after receiving result in CompletableFuture, not after receiving CompletableFuture itself

        final CompletableFuture<Void> outdated = new CompletableFuture<>();
        final CompletableFuture<T> dbResponse = new CompletableFuture<>();
        final OutdatableResult<T> result = new OutdatableResult<>(dbResponse, outdated);

        final OutdatableResult<T> cachedResult = cache.putIfAbsent(key, result);

        if (cachedResult != null) {
            return cachedResult;
        }

        CompletableFuture<T> tCompletableFuture = db.get(key);
        tCompletableFuture.whenComplete((t, throwable) -> {
            if (throwable != null) {
                dbResponse.completeExceptionally(throwable);
            } else {
                dbResponse.complete(t);
            }

            scheduledExecutorService.schedule(
                    () -> {
                        cache.remove(key, result);
                        outdated.complete(null);
                    },
                    timeout,
                    timeoutUnits);
        });

        return result;
    }
}
