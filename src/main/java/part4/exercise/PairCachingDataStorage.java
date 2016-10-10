package part4.exercise;

import part2.cache.CachingDataStorage;

import java.util.function.BiFunction;
import java.util.function.Function;

public class PairCachingDataStorage<K, T, K1, T1, K2, T2> implements CachingDataStorage<K, T> {

    public PairCachingDataStorage(CachingDataStorage<K1, T1> storage1,
                                  CachingDataStorage<K2, T2> storage2,
                                  Function<K, K1> getKey1,
                                  Function<K, K2> getKey2,
                                  Function<K, BiFunction<T1, T2, T>> resultMapper) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public OutdatableResult<T> getOutdatable(K key) {
        // TODO
        throw new UnsupportedOperationException();
    }
}
