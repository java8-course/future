package part2.cache;

import data.typed.Employee;
import data.typed.Employer;
import data.typed.JobHistoryEntry;
import data.typed.Position;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    private CompletableFuture<Employee> typedEmployee(data.Employee empl, ConcurrentLinkedQueue<CompletableFuture<Void>> outdated) {
        System.out.println("Requesting typed data for employee");
        final List<CompletableFuture<JobHistoryEntry>> futureJobs = empl.getJobHistory().parallelStream()
                .map(jhe -> {
                    System.out.println("Request: employer " + jhe.getEmployer());
                    final OutdatableResult<Employer> employer = employerStorage.getOutdatable(jhe.getEmployer());
                    System.out.println("Request: position " + jhe.getPosition());
                    final OutdatableResult<Position> position = positionStorage.getOutdatable(jhe.getPosition());
                    outdated.add(employer.getOutdated());
                    outdated.add(position.getOutdated());

                    return CompletableFuture.allOf(employer.getResult(), position.getResult())
                            .thenApply(v -> {
                                System.out.println("Constructing typed JHE");
                                return new JobHistoryEntry(position.getResult().join(), employer.getResult().join(), jhe.getDuration());
                            });
                }).collect(Collectors.toList());


        System.out.println("Returning typed completable future");
        return CompletableFuture.allOf(futureJobs.toArray(new CompletableFuture[futureJobs.size()])).thenApply(v ->
                futureJobs.parallelStream().map(CompletableFuture::join).collect(Collectors.toList()))
                .thenApply(jhl -> {
                    System.out.println("Creating typed employee");
                    return new Employee(empl.getPerson(), jhl);
                });
    }

    @Override
    public OutdatableResult<Employee> getOutdatable(String key) {
        final ConcurrentLinkedQueue<CompletableFuture<Void>> outdated = new ConcurrentLinkedQueue<>();

        System.out.println("Requesting untyped data for employee");
        final OutdatableResult<data.Employee> untypedEmployee = employeeStorage.getOutdatable(key);

        final CompletableFuture<Employee> futureEmployee = untypedEmployee.getResult()
                .thenCompose(empl -> typedEmployee(empl, outdated));

        outdated.add(untypedEmployee.getOutdated());

        final CompletableFuture<Object> anyOutdated = CompletableFuture.anyOf(outdated.toArray(new CompletableFuture[0]));

        System.out.println("Returning from getOutdatable");
        return new OutdatableResult<>(futureEmployee, anyOutdated.thenRun(() -> System.out.println("Outdating")));
    }
}
