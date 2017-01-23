package part3.exercise;

import part2.cache.CachingDataStorage;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ListCachingDataStorage<K, T> implements CachingDataStorage<List<K>, List<T>> {

    private final CachingDataStorage<K, T> storage;

    public ListCachingDataStorage(CachingDataStorage<K, T> storage) {
        this.storage = storage;
    }

    @Override
    public OutdatableResult<List<T>> getOutdatable(List<K> key) {

        return new OutdatableResult<List<T>>(CompletableFuture.completedFuture(key.stream().map(k -> storage.get(k).join()).collect(Collectors.toList())),
                CompletableFuture.anyOf(key.stream()
                        .map(k -> storage.getOutdatable(k).getOutdated())
                        .collect(Collectors.toList()).toArray(new CompletableFuture[0]))
                        .thenAccept(o -> {}));

    }
}