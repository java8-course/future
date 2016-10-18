package part2.cache;

import com.sun.corba.se.impl.orbutil.threadpool.*;
import com.sun.corba.se.impl.orbutil.threadpool.TimeoutException;
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
        CompletableFuture<T> tCompletableFuture = new CompletableFuture<>();
        CompletableFuture<Void> outdated = new CompletableFuture<>();
        OutdatableResult<T> outdatable = new OutdatableResult<>(tCompletableFuture, outdated);
        OutdatableResult<T> outdatableResult = cache.putIfAbsent(key, outdatable);

        if (outdatableResult != null) {
            return outdatableResult;
        }

        db.get(key)
                .whenComplete((t, throwable) -> {
                    if (throwable != null) {
                        tCompletableFuture.completeExceptionally(throwable);
                    } else {
                        tCompletableFuture.complete(t);
                    }
                    scheduledExecutorService.schedule(
                            () -> {
                                cache.remove(key, outdatable);
                                outdated.complete(null);
                            },
                            timeout,
                            timeoutUnits
                    );
                });

        return outdatable;
    }
}
