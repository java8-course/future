package part2.cache;

import data.typed.JobHistoryEntry;
import data.typed.Employee;
import data.typed.Employer;
import data.typed.Position;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import static java.util.stream.Collectors.toList;

public class TypedEmployeeCachedStorage implements CachingDataStorage<String, data.typed.Employee> {

    private final CachingDataStorage<String, data.Employee> employeeStorage;
    private final CachingDataStorage<String, Position> positionStorage;
    private final CachingDataStorage<String, Employer> employerStorage;

    public TypedEmployeeCachedStorage(CachingDataStorage<String, data.Employee> employeeStorage,
                                      CachingDataStorage<String, Position> positionStorage,
                                      CachingDataStorage<String, Employer> employerStorage) {
        this.employeeStorage = employeeStorage;
        this.positionStorage = positionStorage;
        this.employerStorage = employerStorage;
    }

    @Override
    public OutdatableResult<Employee> getOutdatable(String key) {
        // TODO note that you don't know timeouts for different storage. And timeouts can be different.
        OutdatableResult<data.Employee> employeeOutdatable = employeeStorage.getOutdatable(key);
        CompletableFuture<Void> outdated = new CompletableFuture<>();

        employeeOutdatable.getOutdated().whenComplete(method(outdated));
        CompletableFuture<Employee> employee = employeeOutdatable.getResult().thenComposeAsync((t) -> asyncToTyped(t, outdated));
        return new OutdatableResult<>(employee, outdated);
    }

    private CompletionStage<Employee> asyncToTyped(data.Employee e, CompletableFuture<Void> future) {
        final List<CompletableFuture<JobHistoryEntry>> jobHistoryFutures =
                e.getJobHistory().stream()
                        .map((j) -> asyncToTyped(j, future))
                        .collect(toList());

        return CompletableFuture.allOf(jobHistoryFutures.toArray(new CompletableFuture[0]))
                .thenApplyAsync(x -> {
                    final List<JobHistoryEntry> jobHistory = jobHistoryFutures.stream()
                            .map(TypedEmployeeCachedStorage::getOrNull)
                            .collect(toList());

                    return new data.typed.Employee(e.getPerson(), jobHistory);
                });
    }

    private CompletableFuture<JobHistoryEntry> asyncToTyped(data.JobHistoryEntry j, CompletableFuture<Void> future) {
        OutdatableResult<Employer> employeeOutdatableResult = employerStorage.getOutdatable(j.getEmployer());
        employeeOutdatableResult.getOutdated().whenComplete(method(future));
        OutdatableResult<Position> positionOutdatableResult = positionStorage.getOutdatable(j.getPosition());
        positionOutdatableResult.getOutdated().whenComplete(method(future));
        return employeeOutdatableResult.getResult().thenCombine(
                positionOutdatableResult.getResult(),
                (e, p) -> new JobHistoryEntry(p, e, j.getDuration()));
    }

//    static class Pair {
//        private CompletableFuture<List<JobHistoryEntry>> complitableListOfJobs;
//        private CompletableFuture<Void> outdated;
//    }

    static BiConsumer<Void, Throwable> method(CompletableFuture<Void> outdated) {
        return (vvoid, throwable) -> {
            if (throwable != null) {
                outdated.completeExceptionally(throwable);
            } else {
                outdated.complete(vvoid);
            }
        };
    }

    private static <T> T getOrNull(Future<T> f) {
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e1) {
            e1.printStackTrace();
            return null;
        }
    }
}
