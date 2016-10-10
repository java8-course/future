package db;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SlowBlockingDb<T> implements Closeable {

    private final SlowFutureDb<T> slowFutureDb;

    public SlowBlockingDb(Map<String, T> values) {
        slowFutureDb = new SlowFutureDb<>(values);
    }

    public T get(String key) throws ExecutionException, InterruptedException {
        return slowFutureDb.get(key).get();
    }

    public SlowFutureDb<T> getFutureDb() {
        return slowFutureDb;
    }

    @Override
    public void close() throws IOException {
        slowFutureDb.close();
    }
}
