package part2.cache;

import data.typed.Employee;
import data.typed.Employer;
import data.typed.JobHistoryEntry;
import data.typed.Position;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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


    private CompletableFuture<data.typed.JobHistoryEntry> jheToTyped(data.JobHistoryEntry j, CompletableFuture<Void> outdated) {

        final OutdatableResult<Employer> outdatableEmployer = employerStorage.getOutdatable(j.getEmployer());
        final OutdatableResult<Position> outdatablePosition = positionStorage.getOutdatable(j.getPosition());

        completeOutdated(outdatableEmployer, outdated);
        completeOutdated(outdatablePosition, outdated);
        return employerStorage.get(j.getEmployer())
                .thenCombine(positionStorage.get(j.getPosition()),
                        (e, p) -> new data.typed.JobHistoryEntry(p, e, j.getDuration()));
    }

    private CompletableFuture<data.typed.Employee> employeeToTyped (data.Employee e, CompletableFuture<Void> outdated) {
        final List<CompletableFuture<JobHistoryEntry>> futuresJobHistoryEntries = e.getJobHistory()
                .stream()
                .map(j -> jheToTyped(j, outdated))
                .collect(Collectors.toList());

        return CompletableFuture.allOf(futuresJobHistoryEntries.toArray(new CompletableFuture[0]))
                .thenApplyAsync(j -> {
                    List<data.typed.JobHistoryEntry> jobHistory = futuresJobHistoryEntries.stream()
                            .map(cf -> {
                                try {
                                    return cf.get();
                                }
                                catch (InterruptedException exception) {
                                    exception.printStackTrace();
                                    return null;
                                }
                                catch (ExecutionException exception) {
                                    exception.printStackTrace();
                                    return null;
                                }
                            })
                            .collect(Collectors.toList());

                    return new data.typed.Employee(e.getPerson(), jobHistory);
                });
    }

    private <T> void completeOutdated(OutdatableResult<T> outdatable, CompletableFuture<Void> outdated) {
        outdatable.getOutdated().whenComplete((v, e) -> {
            if (e != null) {
                outdated.completeExceptionally(e);
            } else {
                outdated.complete(v);
            }
        });
    }

    @Override
    public OutdatableResult<Employee> getOutdatable(String key) {

        final OutdatableResult<data.Employee> outdatableNonTypedEmployee = employeeStorage.getOutdatable(key);
        final CompletableFuture<Void> outdated = new CompletableFuture<>();


        final CompletableFuture<data.Employee> futureNotTypedEmployee = outdatableNonTypedEmployee.getResult();
        final CompletableFuture<Employee> futureTypedEmployee =
                futureNotTypedEmployee.thenComposeAsync((e) -> employeeToTyped(e, outdated));

        completeOutdated(outdatableNonTypedEmployee, outdated);

        return new OutdatableResult<>(futureTypedEmployee, outdated);
    }

}