package part2.cache;

import db.DataStorage;

import java.util.concurrent.*;

public class CachingDataStorageImpl<T> implements CachingDataStorage<String, T> {

    private final DataStorage<String, T> db;
    private final int timeout;
    private final TimeUnit timeoutUnits;
    // TODO can we use Map<String, T> here? Why? -  cause we write for a multithreading app
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
        final OutdatableResult<T> result =
                new OutdatableResult<>(new CompletableFuture<>(), new CompletableFuture<>());
        final OutdatableResult<T> previous = cache.putIfAbsent(key, result);

        if (previous != null) {
            return previous;
        }

        db.get(key).whenComplete((value, throwable) -> {

            if (throwable != null) {
                result.getResult().completeExceptionally(throwable);
            } else {
                result.getResult().complete(value);
            }

            scheduledExecutorService.schedule(
                    () -> {
                        cache.remove(key, result);
                        result.getOutdated().complete(null);
                    },
                    timeout,
                    timeoutUnits
            );

        });

        return result;
    }

}