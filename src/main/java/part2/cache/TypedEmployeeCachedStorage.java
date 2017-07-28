package part2.cache;

import data.typed.Employee;
import data.typed.Employer;
import data.typed.JobHistoryEntry;
import data.typed.Position;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.IntFunction;

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

    private OutdatableResult<Employee> asyncToTyped(data.Employee e) {
        final List<OutdatableResult<JobHistoryEntry>> collect = e.getJobHistory().stream()
                .map(this::asyncToTyped)
                .collect(toList());

        return new OutdatableResult<>(
                CompletableFuture.allOf(collect.stream()
                        .map(OutdatableResult::getResult)
                        .toArray((IntFunction<CompletableFuture<?>[]>) CompletableFuture[]::new))
                        .thenApplyAsync(x -> {
                            final List<JobHistoryEntry> jobHistory = collect.stream()
                                    .map(OutdatableResult::getResult)
                                    .map(this::getOrNull)
                                    .collect(toList());
                            return new data.typed.Employee(e.getPerson(), jobHistory);
                        })
                        .thenApply(Function.identity()),
                CompletableFuture.anyOf(collect.stream()
                        .map(OutdatableResult::getOutdated)
                        .toArray((IntFunction<CompletableFuture<?>[]>) CompletableFuture[]::new))
                        .thenApply(x -> null)
        );
    }

    private OutdatableResult<JobHistoryEntry> asyncToTyped(final data.JobHistoryEntry j) {
        final OutdatableResult<Employer> outdatable = employerStorage.getOutdatable(j.getEmployer());
        final OutdatableResult<Position> outdatable1 = positionStorage.getOutdatable(j.getPosition());

        return new OutdatableResult<>(outdatable.getResult().thenCombine(outdatable1.getResult(),
                (e, p) -> new JobHistoryEntry(p, e, j.getDuration())),
                CompletableFuture.anyOf(outdatable.getOutdated(),
                        outdatable1.getOutdated())
                        .thenApply(x -> null));
    }

    private <T> T getOrNull(Future<T> f) {
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e1) {
            e1.printStackTrace();
            return null;
        }
    }

    private void complete(Void res, Throwable ex, OutdatableResult<Employee> result) {
        if (ex != null) {
            result.getOutdated().completeExceptionally(ex);
        } else {
            result.getOutdated().complete(res);
        }
    }

    @Override
    public OutdatableResult<Employee> getOutdatable(String key) {
        final OutdatableResult<data.Employee> outdatable = employeeStorage.getOutdatable(key);

        final CompletableFuture<OutdatableResult<Employee>> future = outdatable.getResult().thenApply(this::asyncToTyped);
        final OutdatableResult<Employee> result = new OutdatableResult<>(new CompletableFuture<>(), new CompletableFuture<>());

        //thenComplete -
        outdatable.getOutdated().whenComplete((res, ex) -> complete(res, ex, result));

        future.whenComplete((res, ex) -> {
            if (ex != null) {
                result.getResult().completeExceptionally(ex);
            } else {
                result.getResult().complete(getOrNull(res.getResult()));
                res.getOutdated().whenComplete((res2, ex2) -> complete(res2, ex2, result));
            }
        });
        return result;
    }
}

