package part2.cache;

import data.typed.Employee;
import data.typed.Employer;
import data.typed.JobHistoryEntry;
import data.typed.Position;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
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

    private OutdatableResult<Employee> typedEmployee(data.Employee empl) {
        final int jobHistorySize = empl.getJobHistory().size();

        // Tried to use concurrent list instead, then converting to array,
        // but it was unpredictably slow and led to randomly slow outdating
        final CompletableFuture[] outdated = new CompletableFuture[jobHistorySize * 2];
        final AtomicInteger index = new AtomicInteger(0);

        final List<CompletableFuture<JobHistoryEntry>> futureJobs = empl.getJobHistory().parallelStream()
                .map(jhe -> {
                    final OutdatableResult<Employer> employer = employerStorage.getOutdatable(jhe.getEmployer());
                    final OutdatableResult<Position> position = positionStorage.getOutdatable(jhe.getPosition());
                    outdated[index.getAndIncrement()] = employer.getOutdated();
                    outdated[index.getAndIncrement()] = position.getOutdated();

                    return CompletableFuture.allOf(employer.getResult(), position.getResult())
                            .thenApply(v -> new JobHistoryEntry(position.getResult().join(), employer.getResult().join(), jhe.getDuration()));
                }).collect(Collectors.toList());


        final CompletableFuture<Employee> futureEmployee = CompletableFuture.allOf(futureJobs.toArray(new CompletableFuture[jobHistorySize])).thenApply(v ->
                futureJobs.parallelStream().map(CompletableFuture::join).collect(Collectors.toList()))
                .thenApply(jhl -> new Employee(empl.getPerson(), jhl));
        return new OutdatableResult<>(futureEmployee, CompletableFuture.anyOf(outdated).thenApply(x -> null));
    }

    @Override
    public OutdatableResult<Employee> getOutdatable(String key) {
        final OutdatableResult<data.Employee> untypedEmployee = employeeStorage.getOutdatable(key);

        final CompletableFuture<Void> eOutdated = untypedEmployee.getOutdated();
        final CompletableFuture<Employee> eComplete = new CompletableFuture<>();

        untypedEmployee.getResult()
                .thenApply(this::typedEmployee)
                .whenComplete((oe, t) -> {
                    if (t != null) {
                        eComplete.completeExceptionally(t);
                    } else {
                        oe.getResult().thenAccept(eComplete::complete);
                        oe.getOutdated().thenAccept(eOutdated::complete);
                    }
                });

        return new OutdatableResult<>(eComplete, eOutdated);
    }
}
