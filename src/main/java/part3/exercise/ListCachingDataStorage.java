package part3.exercise;

import part2.cache.CachingDataStorage;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ListCachingDataStorage<K, T> implements CachingDataStorage<List<K>, List<T>> {
    private final CachingDataStorage<K, T> storage;

    public ListCachingDataStorage(CachingDataStorage<K, T> storage) {
        this.storage = storage;
    }

    private <T> T getOrNull(Future<T> f) {
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e1) {
            e1.printStackTrace();
            return null;
        }
    }

    @Override
    public OutdatableResult<List<T>> getOutdatable(List<K> key) {
        final List<OutdatableResult<T>> collect = key.stream()
                .map(storage::getOutdatable)
                .collect(Collectors.toList());

        final CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(collect.stream()
                .map(OutdatableResult::getResult)
                .toArray(CompletableFuture[]::new));

        return new OutdatableResult<>(voidCompletableFuture.thenApply(v -> collect.stream()
                .map(OutdatableResult::getResult)
                .map(this::getOrNull)
                .collect(Collectors.toList())),
                CompletableFuture.anyOf(collect.stream()
                        .map(OutdatableResult::getOutdated)
                        .toArray(CompletableFuture[]::new))
                        .thenApply(v -> null));
    }
}
