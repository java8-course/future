package part2.cache;

import db.DataStorage;
import db.SlowCompletableFutureDb;

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

        CompletableFuture<Void> sched = new CompletableFuture<>();
        CompletableFuture<T> resultFuture = new CompletableFuture<>();
        OutdatableResult<T> outdatableResult = new OutdatableResult<>(resultFuture, sched);
        OutdatableResult<T> cashedResult = cache.putIfAbsent(key, outdatableResult);
        if (cashedResult == null) {
            db.get(key).whenComplete((result, throwable) -> {
                if (throwable == null){
                    resultFuture.complete(result);
                }
                else {
                    resultFuture.completeExceptionally(throwable);
                }
                scheduledExecutorService.schedule(() -> {
                    cache.remove(key, outdatableResult);
                    sched.complete(null);
                }, timeout, timeoutUnits);
            });
            return outdatableResult;
        } else return cashedResult;
    }
}
