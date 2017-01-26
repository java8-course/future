package part2.cache;

import data.typed.Employee;
import data.typed.Employer;
import data.typed.Position;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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

    @Override
    public OutdatableResult<data.typed.Employee> getOutdatable(String key) {
        // TODO note that you don't know timeouts for different storage. And timeouts can be different.
        final OutdatableResult<data.Employee> notTypedOutdatable = employeeStorage.getOutdatable(key);
        final CompletableFuture<Employee> typedEmployeeCf = new CompletableFuture<>();
        final CompletableFuture<Void> outdatedCf = new CompletableFuture<>();

        notTypedOutdatable.getResult().whenComplete((emp, ex) -> {
            if (ex != null) {
                typedEmployeeCf.completeExceptionally(ex);
                return;
            }

            final List<CompletableFuture<Void>> outdatedSignals = new ArrayList<>();
            outdatedSignals.add(notTypedOutdatable.getOutdated());
            final List<CompletableFuture<data.typed.JobHistoryEntry>> jobHistoryCf = emp.getJobHistory().stream()
                    .map(jhe -> {
                        final OutdatableResult<Position> positionResult = positionStorage.getOutdatable(jhe.getPosition());
                        outdatedSignals.add(positionResult.getOutdated());
                        final OutdatableResult<Employer> employerResult = employerStorage.getOutdatable(jhe.getEmployer());
                        outdatedSignals.add(employerResult.getOutdated());
                        return positionResult.getResult().thenCombine(employerResult.getResult(),
                                (position, employer) -> new data.typed.JobHistoryEntry(position, employer, jhe.getDuration()));
                    })
                    .collect(Collectors.toList());
            CompletableFuture.anyOf(outdatedSignals.toArray(new CompletableFuture[0])).whenComplete((v, t) -> {
                if (t != null) {
                    outdatedCf.completeExceptionally(t);
                } else {
                    outdatedCf.complete((Void) v);
                }
            });
            CompletableFuture.allOf(jobHistoryCf.toArray(new CompletableFuture[0])).whenComplete((v, t) -> {
                if (t != null) {
                    typedEmployeeCf.completeExceptionally(t);
                    return;
                }
                final List<data.typed.JobHistoryEntry> typedJobHistory = jobHistoryCf.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList());
                typedEmployeeCf.complete(new Employee(emp.getPerson(), typedJobHistory));
            });
        });
        return new OutdatableResult<>(typedEmployeeCf, outdatedCf);
    }
}
