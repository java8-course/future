package part3.exercise;

import part2.cache.CachingDataStorage;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

public class PairCachingDataStorage<K, T, K1, T1, K2, T2> implements CachingDataStorage<K, T> {

    CachingDataStorage<K1, T1> storage1;
    CachingDataStorage<K2, T2> storage2;
    Function<K, K1> getKey1;
    Function<K, K2> getKey2;
    Function<K, BiFunction<T1, T2, T>> resultMapper;

    public PairCachingDataStorage(CachingDataStorage<K1, T1> storage1,
                                  CachingDataStorage<K2, T2> storage2,
                                  Function<K, K1> getKey1,
                                  Function<K, K2> getKey2,
                                  Function<K, BiFunction<T1, T2, T>> resultMapper) {
        this.storage1 = storage1;
        this.storage2 = storage2;
        this.getKey1 = getKey1;
        this.getKey2 = getKey2;
        this.resultMapper = resultMapper;
    }

    @Override
    public OutdatableResult<T> getOutdatable(K key) {

        return new OutdatableResult<>(CompletableFuture
                .completedFuture(resultMapper.apply(key).apply(storage1.get(getKey1.apply(key)).join(),
                        storage2.get(getKey2.apply(key)).join())),
                new CompletableFuture<>());
    }
}
