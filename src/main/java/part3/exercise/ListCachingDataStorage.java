package part3.exercise;

import part2.cache.CachingDataStorage;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ListCachingDataStorage<K, T> implements CachingDataStorage<List<K>, List<T>> {

    private CachingDataStorage<K,T> storage;

    public ListCachingDataStorage(CachingDataStorage<K, T> storage) {
        this.storage = storage;
    }

    @Override
    public OutdatableResult<List<T>> getOutdatable(List<K> key) {
        List<OutdatableResult<T>> collect = key.stream()
                .map(storage::getOutdatable)
                .collect(Collectors.toList());

        List<CompletableFuture<Void>> outdatables = collect.stream()
                .map(OutdatableResult::getOutdated)
                .collect(Collectors.toList());

        List<CompletableFuture<T>> results = collect.stream()
                .map(OutdatableResult::getResult)
                .collect(Collectors.toList());

        CompletableFuture<List<T>> listCompletableFuture = CompletableFuture.allOf(results.toArray(new CompletableFuture[0]))
                .thenApply(v -> results.stream()
                        .map(ListCachingDataStorage::getOrNull)
                        .collect(Collectors.toList()));

        return new OutdatableResult<>(
                listCompletableFuture,
                CompletableFuture.anyOf(outdatables.toArray(new CompletableFuture[0])).thenApply(v -> null)
        );
    }

    private static <T> T getOrNull(Future<T> f) {
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e1) {
            e1.printStackTrace();
            return null;
        }
    }
}
