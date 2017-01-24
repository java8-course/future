package part3.exercise;

import part2.cache.CachingDataStorage;

import java.util.function.Function;

public class ComposeCachingDataStorage<K1, T1, K2, T2> implements CachingDataStorage<K1, T2> {

    private final CachingDataStorage<K1, T1> storage1;
    private final CachingDataStorage<K2, T2> storage2;
    private final Function<T1, K2> mapping;

    public ComposeCachingDataStorage(CachingDataStorage<K1, T1> storage1,
                                     CachingDataStorage<K2, T2> storage2,
                                     Function<T1, K2> mapping) {
        this.storage1 = storage1;
        this.storage2 = storage2;
        this.mapping = mapping;
    }

    @Override
    public OutdatableResult<T2> getOutdatable(K1 key) {

        return storage2.getOutdatable(storage1.get(key).thenApply(mapping).join());
    }
}
