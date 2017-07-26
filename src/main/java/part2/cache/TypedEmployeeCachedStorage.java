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

        final List<CompletableFuture<JobHistoryEntry>> jobHistoryFutures =
                e.getJobHistory().stream()
                        .map(this::asyncToTyped)
                        .collect(toList());

        final List<CompletableFuture> outdatedList = e.getJobHistory().stream()
                .map(this::getOutDated)
                .collect(toList());


        return new OutdatableResult<>(
                CompletableFuture.allOf(jobHistoryFutures.toArray(new CompletableFuture[0]))
                        .thenApplyAsync(x -> {
                            final List<JobHistoryEntry> jobHistory = jobHistoryFutures.stream()
                                    .map(this::getOrNull)
                                    .collect(toList());
                            return new data.typed.Employee(e.getPerson(), jobHistory);
                        })
                        .thenApply(Function.identity()),
                CompletableFuture.anyOf(outdatedList.toArray(new CompletableFuture[0]))
                        .thenApply(x -> null)
        );
    }

    private CompletableFuture<JobHistoryEntry> asyncToTyped(data.JobHistoryEntry j) {
        return employerStorage.get(j.getEmployer())
                .thenCombine(
                        positionStorage.get(j.getPosition()),
                        (e, p) -> new JobHistoryEntry(p, e, j.getDuration()));
    }

    private CompletableFuture getOutDated(data.JobHistoryEntry j) {
        return CompletableFuture.anyOf(positionStorage.getOutdatable(j.getPosition()).getOutdated(),
                employerStorage.getOutdatable(j.getEmployer()).getOutdated());
    }

    private <T> T getOrNull(Future<T> f) {
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e1) {
            e1.printStackTrace();
            return null;
        }
    }

    @Override
    public OutdatableResult<Employee> getOutdatable(String key) {
        final OutdatableResult<data.Employee> outdatable = employeeStorage.getOutdatable(key);
        final CompletableFuture<Employee> employeeResult = outdatable.getResult().thenCompose(e -> asyncToTyped(e).getResult());
        final CompletableFuture<Void> employeeOutdated = outdatable.getResult().thenCompose(e -> asyncToTyped(e).getOutdated());
        outdatable.getResult().thenCompose(e -> asyncToTyped(e).getOutdated());
        return new OutdatableResult<>(employeeResult, outdatable.getOutdated().applyToEither(employeeOutdated, Function.identity()));
    }
}

