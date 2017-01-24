package part3.exercise;

import part2.cache.CachingDataStorage;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
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

    private BiConsumer<Void, Throwable> completeOutdated(CompletableFuture<Void> voidCompletableFuture) {
        return (v, e) -> {
            if (e != null) {
                voidCompletableFuture.completeExceptionally(e);
            }
            else {
                voidCompletableFuture.complete(null);
            }
        };
    }


    @Override
    public OutdatableResult<T2> getOutdatable(K1 key) {

        final CompletableFuture<Void> outdated = new CompletableFuture<>();

        final OutdatableResult<T1> t1OutdatableResult = storage1.getOutdatable(key);
        final OutdatableResult<T2> t2OutdatableResult =
                storage2.getOutdatable(t1OutdatableResult.getResult().thenApply(mapping).join());

        t1OutdatableResult.getOutdated().whenComplete(completeOutdated(outdated));
        t2OutdatableResult.getOutdated().whenComplete(completeOutdated(outdated));

        return new OutdatableResult<T2>(t2OutdatableResult.getResult(), outdated);
    }
}