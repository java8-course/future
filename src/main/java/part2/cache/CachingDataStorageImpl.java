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
                public Thread newThread (Runnable r) {
                    final Thread thread = threadFactory.newThread(r);
                    thread.setDaemon(true);
                    return thread;
                }
            });

    public CachingDataStorageImpl (DataStorage<String, T> db, int timeout, TimeUnit timeoutUnits) {
        this.db = db;
        this.timeout = timeout;
        this.timeoutUnits = timeoutUnits;
    }

    @Override
    public OutdatableResult<T> getOutdatable (String key) {
        CompletableFuture<T> result = new CompletableFuture<>();
        CompletableFuture<Void> outdated = new CompletableFuture<>();
        OutdatableResult<T> outdateResult = new OutdatableResult<>(result, outdated);
        OutdatableResult<T> cacheResult = cache.putIfAbsent(key, outdateResult);

        if (cacheResult != null) {
            return outdateResult;
        }

        db.get(key)
            .whenComplete((t, throwable) -> {
                if (throwable != null) {
                    result.completeExceptionally(throwable);
                } else {
                    result.complete(t);
                }
                scheduledExecutorService.schedule(() -> {
                        cache.remove(key, outdateResult);
                        outdated.complete(null);
                    },
                    timeout,
                    timeoutUnits);
            });
        return outdateResult;
    }
}