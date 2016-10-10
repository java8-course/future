package part2.cache;

import db.DataStorage;

import java.util.concurrent.CompletableFuture;

public interface CachingDataStorage<K, T> extends DataStorage<K, T> {

    class OutdatableResult<T> {
        private final CompletableFuture<T> result;
        private final CompletableFuture<Void> outdated;

        public OutdatableResult(CompletableFuture<T> result, CompletableFuture<Void> outdated) {
            this.result = result;
            this.outdated = outdated;
        }

        public CompletableFuture<T> getResult() {
            return result;
        }

        public CompletableFuture<Void> getOutdated() {
            return outdated;
        }
    }

    OutdatableResult<T> getOutdatable(K key);

    @Override
    default CompletableFuture<T> get(K key) {
        return getOutdatable(key).getResult();
    }
}
