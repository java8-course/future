package db;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class SlowCompletableFutureDb<T> implements DataStorage<String, T>, Closeable {

    private volatile Map<String, T> values;
    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor();
    private int maxTimeout;
    private TimeUnit timeoutUnits;

    public SlowCompletableFutureDb(Map<String, T> values,
                                   int maxTimeout,
                                   TimeUnit timeoutUnits) {
        this.values = new HashMap<>(values);
        this.maxTimeout = maxTimeout;
        this.timeoutUnits = timeoutUnits;
    }

    public SlowCompletableFutureDb(Map<String, T> values) {
        this(values, 100, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<T> get(String key) {
        final T value = values.get(key);
        final CompletableFuture<T> result = new CompletableFuture<T>();

        final int timeout = ThreadLocalRandom.current().nextInt(maxTimeout);

        scheduledExecutorService.schedule(
                () -> result.complete(value),
                timeout,
                timeoutUnits);

        return result;
    }

    public void setValues(Map<String, T> values) {
        this.values = new HashMap<>(values);
    }

    @Override
    public void close() throws IOException {
        scheduledExecutorService.shutdown();
        scheduledExecutorService.shutdownNow();
        try {
            scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
