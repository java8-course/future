package part4.exercise;

import part2.cache.CachingDataStorage;

import java.util.function.Function;

public class ComposeCachingDataStorage<K1, T1, K2, T2> implements CachingDataStorage<K1, T2> {

    public ComposeCachingDataStorage(CachingDataStorage<K1, T1> storage1,
                                     CachingDataStorage<K2, T2> storage2,
                                     Function<T1, K2> mapping) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public OutdatableResult<T2> getOutdatable(K1 key) {
        // TODO
        throw new UnsupportedOperationException();
    }
}
