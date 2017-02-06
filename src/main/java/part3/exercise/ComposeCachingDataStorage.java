package part3.exercise;

import part2.cache.CachingDataStorage;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class ComposeCachingDataStorage<K1, T1, K2, T2> implements CachingDataStorage<K1, T2> {

    private CachingDataStorage<K1, T1> storage1;
    private CachingDataStorage<K2, T2> storage2;
    private Function<T1, K2> mapping;

    public ComposeCachingDataStorage (CachingDataStorage<K1, T1> storage1,
                                      CachingDataStorage<K2, T2> storage2,
                                      Function<T1, K2> mapping) {
        this.storage1 = storage1;
        this.storage2 = storage2;
        this.mapping = mapping;
    }

    @Override
    public OutdatableResult<T2> getOutdatable (K1 key) {

        final CompletableFuture<Void> outdated = new CompletableFuture<>();
        final OutdatableResult<T1> outdatableT1 = storage1.getOutdatable(key);
        final CompletableFuture<T1> resultT1 = outdatableT1.getResult();
        final CompletableFuture<K2> resultK2 = resultT1.thenApply(mapping);

        final CompletableFuture<OutdatableResult<T2>> outdatableResultCompletableFuture = resultK2.thenApply(x -> storage2.getOutdatable(x));
        final CompletableFuture<T2> cfT2 = outdatableResultCompletableFuture
                .thenCompose(OutdatableResult::getResult);

        outdatableResultCompletableFuture.thenCompose(OutdatableResult::getOutdated)
                .whenComplete(whenComplete(outdated));

        return new OutdatableResult<>(cfT2, outdated);
    }

    private BiConsumer<Void, Throwable> whenComplete (CompletableFuture<Void> outdated) {
        return (value, exception) -> {
            if (exception != null) {
                outdated.completeExceptionally(exception);
            } else {
                outdated.complete(null);
            }
        };
    }
}
