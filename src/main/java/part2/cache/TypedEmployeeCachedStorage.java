package part2.cache;

import data.typed.Employee;
import data.typed.Employer;
import data.typed.Position;
import data.typed.JobHistoryEntry;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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

// get data.Employee
// get list jobHistoryEntry and
//        get Position
//        get Employer
// create data.typed.Employee

        OutdatableResult<data.Employee> employeeOutdatableResult = employeeStorage.getOutdatable(key);
        CompletableFuture<data.Employee> employee = employeeOutdatableResult.getResult();
        CompletableFuture<Void> employeeOutdated = employeeOutdatableResult.getOutdated();

        CompletableFuture<Employee> employeeFuture = employee.thenCompose(empl -> {
            List<OutdatableResult<JobHistoryEntry>> outdatableJobs = getListOutdatableJobHistoryEntry(empl);

            CompletableFuture[] futuresJobHistoryEntryCompleted = outdatableJobs.stream()
                    .map(OutdatableResult::getResult)
                    .toArray(CompletableFuture[]::new);

            CompletableFuture<Employee> employeeTypedFuture = CompletableFuture.allOf(futuresJobHistoryEntryCompleted)
                    .thenApply(aVoid -> new Employee(
                            empl.getPerson(),
                            outdatableJobs.stream()
                                    .map(OutdatableResult::getResult)
                                    .map(CompletableFuture::join)
                                    .collect(toList())));

            CompletableFuture[] futureJobHistoryEntryOutdated = outdatableJobs.stream()
                    .map(OutdatableResult::getOutdated)
                    .toArray(CompletableFuture[]::new);

            CompletableFuture<Void> jobHistoryOutdated = CompletableFuture.anyOf(futureJobHistoryEntryOutdated)
                    .thenAccept(o -> {
                    });

            jobHistoryOutdated.thenAccept(employeeOutdated::complete);

            return employeeTypedFuture;
        });

        return new OutdatableResult<>(employeeFuture, employeeOutdated);
    }

    private List<OutdatableResult<JobHistoryEntry>> getListOutdatableJobHistoryEntry (data.Employee employee) {
        return employee.getJobHistory().stream()
                .map(jobHistoryEntry -> {
                    OutdatableResult<Position> positionOutdatable = positionStorage.getOutdatable(jobHistoryEntry.getPosition());
                    OutdatableResult<Employer> employerOutdatable = employerStorage.getOutdatable(jobHistoryEntry.getEmployer());

                    CompletableFuture<JobHistoryEntry> jobHistoryEntryCompletableFuture = positionOutdatable.getResult()
                            .thenCombine(employerOutdatable.getResult(),
                                    (position, employer) -> new JobHistoryEntry(
                                            position,
                                            employer,
                                            jobHistoryEntry.getDuration()));

                    CompletableFuture<Void> jobHistoryEntryOutdated = positionOutdatable.getOutdated().acceptEither(
                            employerOutdatable.getOutdated(),
                            aVoid -> {
                            });

                    return new OutdatableResult<>(
                            jobHistoryEntryCompletableFuture,
                            jobHistoryEntryOutdated
                    );
                })
                .collect(toList());
    }


}
