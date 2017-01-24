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

        final List<OutdatableResult<T>> outdatableResults = key.stream()
                .map(storage::getOutdatable)
                .collect(Collectors.toList());

        return new OutdatableResult<List<T>>(CompletableFuture.completedFuture(outdatableResults.stream()
                .map(or -> or.getResult().join())
                .collect(Collectors.toList())),

                CompletableFuture.anyOf(outdatableResults.stream()
                                        .map(OutdatableResult::getOutdated)
                                        .toArray(i -> (CompletableFuture<Void>[]) new CompletableFuture[i]))
                        .thenAccept(o -> {}));
    }
}