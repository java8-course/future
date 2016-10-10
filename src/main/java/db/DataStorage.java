package db;

import java.util.concurrent.CompletableFuture;

public interface DataStorage<K, T> {
    CompletableFuture<T> get(K key);
}
