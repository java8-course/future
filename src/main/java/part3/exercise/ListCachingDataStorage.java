package part3.exercise;

import part2.cache.CachingDataStorage;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;

public class ListCachingDataStorage<K, T> implements CachingDataStorage<List<K>, List<T>> {

    CachingDataStorage<K, T> storage;

    public ListCachingDataStorage(CachingDataStorage<K, T> storage) {
        this.storage = storage;
    }

    @Override
    public OutdatableResult<List<T>> getOutdatable(List<K> key) {
        final List<OutdatableResult<T>> outdatableList = key.stream()
                .map(x -> storage.getOutdatable(x))
                .collect(toList());

        final CompletableFuture<T>[] rcf = outdatableList.stream()
                .map(OutdatableResult::getResult)
                .toArray(i -> (CompletableFuture<T>[]) new CompletableFuture[i]);
        final CompletableFuture<List<T>> lcf = CompletableFuture.allOf(rcf)
                .thenApply((v) -> outdatableList.stream()
                        .map(OutdatableResult::getResult)
                        .map(CompletableFuture::join)
                        .collect(toList()));

        final CompletableFuture<T>[] ocf = outdatableList.stream()
                .map(OutdatableResult::getOutdated)
                .toArray(i -> (CompletableFuture<T>[]) new CompletableFuture[i]);
        final CompletableFuture<Void> outdated = CompletableFuture.anyOf(ocf)
                .thenRun(()->{});

        return new OutdatableResult<>(lcf, outdated);
    }
}
