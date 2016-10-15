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
        final CompletableFuture<Void> outdated = new CompletableFuture<>();
        final List<CompletableFuture<T>> futureResults = key.stream().map(storage::getOutdatable).map(response -> {
            response.getOutdated().thenAccept(outdated::complete);
            return response.getResult();
        }).collect(Collectors.toList());

        final CompletableFuture<List<T>> resultList = CompletableFuture.allOf(futureResults.toArray(new CompletableFuture[key.size()])).thenApply(
                v -> futureResults.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));

        return new OutdatableResult<>(resultList, outdated);
    }
}
