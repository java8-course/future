package part2.cache;

import db.DataStorage;
import db.SlowBlockingDb;
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
       OutdatableResult<T> result = new OutdatableResult<>(new CompletableFuture<>(), new CompletableFuture<>());
       OutdatableResult<T> outDated = cache.putIfAbsent(key, result);

       if (outDated == null) {
           db.get(key).whenComplete((res, exception) -> {
               if (exception != null) {
                   result.getResult().completeExceptionally(exception);
               } else {
                   result.getResult().complete(res);
               }

               scheduledExecutorService.schedule(() -> {
                           cache.remove(get(key), result);
                           result.getOutdated().complete(null);
                       },
                       timeout,
                       timeoutUnits);
           });
           return result;
       }
       return outDated;
    }
}
