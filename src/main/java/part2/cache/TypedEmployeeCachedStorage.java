package part2.cache;

import data.typed.Employee;
import data.typed.Employer;
import data.typed.JobHistoryEntry;
import data.typed.Position;
import part3.exercise.MappingCachingDataStorage;
import part3.exercise.PairCachingDataStorage;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
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

        PairCachingDataStorage <data.JobHistoryEntry, data.typed.JobHistoryEntry, String, Employer, String, Position> pairCachingDataStorage=
                new PairCachingDataStorage <data.JobHistoryEntry, data.typed.JobHistoryEntry, String, Employer, String, Position> (employerStorage, positionStorage,
                k -> j.getEmployer(), k -> j.getPosition(),
                k -> (e, p) -> new data.typed.JobHistoryEntry(p, e, j.getDuration()));

        return pairCachingDataStorage.getOutdatable(j).getResult();
    }

    private CompletableFuture<data.typed.Employee> employeeToTyped (data.Employee e, CompletableFuture<Void> outdated) {

        final List<CompletableFuture<JobHistoryEntry>> futuresJobHistoryEntries = e.getJobHistory()
                .stream()
                .map(j -> jheToTyped(j, outdated))
                .collect(Collectors.toList());

        
        return CompletableFuture.allOf(futuresJobHistoryEntries.toArray(new CompletableFuture[0]))
                .thenApplyAsync(j -> {
                    List<data.typed.JobHistoryEntry> jobHistory = futuresJobHistoryEntries.stream()
                            .map(CompletableFuture::join)
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

        MappingCachingDataStorage <String, String, data.Employee, data.typed.Employee> mappingCachingDataStorage = 
                new MappingCachingDataStorage<String, String, data.Employee, data.typed.Employee>
                        (employeeStorage, Function.identity(), (k, e) -> employeeToTyped(e, outdated).join());

        final OutdatableResult<Employee> outdatable = mappingCachingDataStorage.getOutdatable(key);

        completeOutdated(outdatableNonTypedEmployee, outdated);

        return outdatable;
    }

}