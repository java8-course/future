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
        final OutdatableResult<T> newResult
                = new OutdatableResult<>(new CompletableFuture<>(), new CompletableFuture<>());

        final OutdatableResult<T> cashed =
                cache.putIfAbsent(key, newResult);
        if (cashed != null) {
            return cashed;
        }

        db.get(key).whenComplete((res, ex) -> {
            if (ex != null) {
                newResult.getResult().completeExceptionally(ex);
            } else {
                newResult.getResult().complete(res);
            }
            scheduledExecutorService.schedule(() -> {
                cache.remove(key, newResult);
                newResult.getOutdated().complete(null);
            }, timeout, timeoutUnits);
        });

        return newResult;
    }
}
