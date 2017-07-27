package part3.exercise;

import part2.cache.CachingDataStorage;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class ComposeCachingDataStorage<K1, T1, K2, T2> implements CachingDataStorage<K1, T2> {


    private CachingDataStorage<K1, T1> storage1;
    private CachingDataStorage<K2, T2> storage2;
    private Function<T1, K2> mapping;

    public ComposeCachingDataStorage(CachingDataStorage<K1, T1> storage1, CachingDataStorage<K2, T2> storage2, Function<T1, K2> mapping) {
        this.storage1 = storage1;
        this.storage2 = storage2;
        this.mapping = mapping;
    }

    @Override
    public OutdatableResult<T2> getOutdatable(K1 key) {

        OutdatableResult<T1> outdatableFromStorage1 = storage1.getOutdatable(key);

        CompletableFuture<OutdatableResult<T2>> outdatableResultCompletableFuture = outdatableFromStorage1.getResult().thenApply(
                v -> storage2.getOutdatable(mapping.apply(v))
        );

        OutdatableResult<T2> result = new OutdatableResult<>(new CompletableFuture<>(), new CompletableFuture<>());

        outdatableResultCompletableFuture.thenAccept(
                v -> {
                    v.getResult().thenAccept(
                            res -> {
                                result.getResult().complete(res);
                                outdatableFromStorage1.getOutdated().runAfterEither(v.getOutdated(), () -> result.getOutdated().complete(null));
                            }
                    );
                }
        );

        return result;
    }

}
