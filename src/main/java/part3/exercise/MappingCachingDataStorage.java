package part3.exercise;

import part2.cache.CachingDataStorage;

import java.util.function.BiFunction;
import java.util.function.Function;

public class MappingCachingDataStorage<K, K1, T1, T> implements CachingDataStorage<K, T> {

    private final CachingDataStorage<K1, T1> storage;
    private final Function<K, K1> mapKey;
    private final BiFunction<K, T1, T> mapValue;

    public MappingCachingDataStorage(CachingDataStorage<K1, T1> storage,
                                     Function<K, K1> mapKey,
                                     BiFunction<K, T1, T> mapValue) {
        this.storage = storage;
        this.mapKey = mapKey;
        this.mapValue = mapValue;
    }

    @Override
    public OutdatableResult<T> getOutdatable(K key) {
        final OutdatableResult<T1> response = storage.getOutdatable(mapKey.apply(key));
        return new OutdatableResult<>(
                response.getResult().thenApply(t1 -> mapValue.apply(key, t1)),
                response.getOutdated());
    }
}
