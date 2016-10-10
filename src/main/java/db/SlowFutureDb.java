package db;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

public class SlowFutureDb<T> implements Closeable {

    private final SlowCompletableFutureDb<T> slowCompletableFutureDb;

    public SlowFutureDb(Map<String, T> values) {
        slowCompletableFutureDb = new SlowCompletableFutureDb<>(values);
    }

    public Future<T> get(String key) {
        return slowCompletableFutureDb.get(key);
    }

    public SlowCompletableFutureDb<T> getCompletableFutureDb() {
        return slowCompletableFutureDb;
    }

    @Override
    public void close() throws IOException {
        slowCompletableFutureDb.close();
    }
}
