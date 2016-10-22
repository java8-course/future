package part2.cache;

import data.typed.Employee;
import data.typed.Employer;
import data.typed.JobHistoryEntry;
import data.typed.Position;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

    private CompletionStage<Employee> asyncToTyped(data.Employee employee, CompletableFuture<Void> outdated) {
        final List<CompletableFuture<JobHistoryEntry>> jobHistoryFutures =
                employee.getJobHistory().stream()
                        .map(x -> asyncToTyped(x, outdated))
                        .collect(toList());

        return CompletableFuture.allOf(jobHistoryFutures.toArray(new CompletableFuture[0]))
                .thenApplyAsync(x -> {
                    final List<JobHistoryEntry> jobHistory = jobHistoryFutures.stream()
                            .map(TypedEmployeeCachedStorage::getOrNull)
                            .collect(toList());

                    return new data.typed.Employee(employee.getPerson(), jobHistory);
                });
    }

    private CompletableFuture<JobHistoryEntry> asyncToTyped(data.JobHistoryEntry j, CompletableFuture<Void> outdated) {

        final OutdatableResult<Employer> outdatableEmployer = employerStorage.getOutdatable(j.getEmployer());
        final OutdatableResult<Position> outdatablePosition = positionStorage.getOutdatable(j.getPosition());

        completeOutdated(outdatableEmployer, outdated);
        completeOutdated(outdatablePosition, outdated);

        return employerStorage.get(j.getEmployer())
                .thenCombine(positionStorage.get(j.getPosition()),
                        (e, p) -> new JobHistoryEntry(p, e, j.getDuration()));
    }

    private <T> void completeOutdated(OutdatableResult<T> outdatable, CompletableFuture<Void> outdated) {
        outdatable.getOutdated().whenComplete((value, exception) -> {
            if (exception != null) {
                outdated.completeExceptionally(exception);
            } else {
                outdated.complete(value);
            }
        });
    }
    private static <T> T getOrNull(Future<T> f) {
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e1) {
            e1.printStackTrace();
            return null;
        }
    }

    @Override
    public OutdatableResult<Employee> getOutdatable(String key) {
        final CompletableFuture<Void> outdated = new CompletableFuture<>();
        final OutdatableResult<data.Employee> outdatable = employeeStorage.getOutdatable(key);
        final CompletableFuture<data.Employee> employee = outdatable.getResult();
        final CompletableFuture<Employee> ecf =
                employee.thenComposeAsync((e) -> asyncToTyped(e, outdated));

        completeOutdated(outdatable, outdated);

        return new OutdatableResult<>(ecf, outdated);
    }

}