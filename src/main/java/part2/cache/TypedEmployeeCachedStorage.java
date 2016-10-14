package part2.cache;

import data.Person;
import data.typed.Employee;
import data.typed.Employer;
import data.typed.JobHistoryEntry;
import data.typed.Position;
import jdk.nashorn.internal.scripts.JO;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        final CompletableFuture<JobHistoryEntry>[] jobHistoryArray = empl.getJobHistory().stream()
                .map(jhe -> {
                    final OutdatableResult<Employer> employer = employerStorage.getOutdatable(jhe.getEmployer());
                    final OutdatableResult<Position> position = positionStorage.getOutdatable(jhe.getPosition());
                    outdated.add(employer.getOutdated());
                    outdated.add(position.getOutdated());

                    return CompletableFuture.allOf(employer.getResult(), position.getResult())
                            .thenApply(v -> new JobHistoryEntry(position.getResult().join(), employer.getResult().join(), jhe.getDuration()));
                }).toArray((IntFunction<CompletableFuture<JobHistoryEntry>[]>) CompletableFuture[]::new);


        return null;
    }

    @Override
    public OutdatableResult<Employee> getOutdatable(String key) {
        final ConcurrentLinkedQueue<CompletableFuture<Void>> outdated = new ConcurrentLinkedQueue<>();

        final OutdatableResult<data.Employee> untypedEmployee = employeeStorage.getOutdatable(key);

        final CompletableFuture<Employee> futureEmployee = untypedEmployee.getResult()
                .thenCompose(empl -> typedEmployee(empl, outdated));

        outdated.add(untypedEmployee.getOutdated());

        final CompletableFuture<Object> anyOutdated = CompletableFuture.anyOf(outdated.toArray(new CompletableFuture[0]));

        return new OutdatableResult<>(futureEmployee, anyOutdated.thenRun(() -> {
        }));
    }
}
