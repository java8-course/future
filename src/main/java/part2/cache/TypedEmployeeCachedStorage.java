package part2.cache;

import data.typed.Employee;
import data.typed.Employer;
import data.typed.JobHistoryEntry;
import data.typed.Position;
import db.SlowCompletableFutureDb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

//        //If it is almost in cache
//        OutdatableResult<Employee> employeeOutdatableFromCache = outdatablesEmployeeCache.get(key);
//        if (employeeOutdatableFromCache!=null){
//            return employeeOutdatableFromCache;
//        }

        OutdatableResult<data.Employee> emplOutdatable = employeeStorage.getOutdatable(key);
        CompletableFuture<Void> outdated = new CompletableFuture<>();

        emplOutdatable
                .getOutdated()
                .whenComplete(divaricator(outdated));

        CompletableFuture<Employee> employeeCompletableFuture = emplOutdatable
                .getResult()
                .thenCompose((o) -> asyncToTyped(o, outdated));

        return new OutdatableResult<>(employeeCompletableFuture, outdated);

    }

    private CompletionStage<Employee> asyncToTyped(data.Employee e, CompletableFuture<Void> cf) {

        final List<CompletableFuture<JobHistoryEntry>> jobHistoryFutures =
                e.getJobHistory()
                        .stream()
                        .map((jjj) -> asyncToTyped(jjj, cf))
                        .collect(toList());

        return CompletableFuture.allOf(jobHistoryFutures.toArray(new CompletableFuture[0]))
                .thenApplyAsync(x -> {
                    final List<JobHistoryEntry> jobHistory = jobHistoryFutures.stream()
                            .map(TypedEmployeeCachedStorage::getOrNull)
                            .collect(toList());
                    return new data.typed.Employee(e.getPerson(), jobHistory);
                });

    }

    private CompletableFuture<JobHistoryEntry> asyncToTyped(data.JobHistoryEntry j, CompletableFuture<Void> cf) {
        OutdatableResult<Employer> empOutdatableResult = employerStorage.getOutdatable(j.getEmployer());
        empOutdatableResult
                .getOutdated()
                .whenComplete(divaricator(cf));

        OutdatableResult<Position> posOutdatableResult = positionStorage.getOutdatable(j.getPosition());
        posOutdatableResult
                .getOutdated()
                .whenComplete(divaricator(cf));

        return empOutdatableResult
                .getResult()
                .thenCombine(
                        posOutdatableResult.getResult(),
                        (emp, pos) -> new JobHistoryEntry(pos, emp, j.getDuration())
                );

//        final SlowCompletableFutureDb<Employer> employersDb = blockingEmployers.getFutureDb().getCompletableFutureDb();
//        final SlowCompletableFutureDb<Position> positionDb = blockingPositions.getFutureDb().getCompletableFutureDb();
//
//        return employersDb.get(j.getEmployer())
//                .thenCombine(
//                        positionDb.get(j.getPosition()),
//                        (e, p) -> new JobHistoryEntry(p, e, j.getDuration()));
    }

    private static BiConsumer<Void, Throwable> divaricator(CompletableFuture<Void> outDated){
        return (voidd, e) -> {
            if (exceptionWasBeen(e)){
                outDated.completeExceptionally(e);
            } else {
                outDated.complete(voidd);
            }
        };
    }
    
    private static boolean exceptionWasBeen(Throwable e){
        return e!=null;
    }

    private static <T> T getOrNull(Future<T> f) {
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e1) {
            e1.printStackTrace();
            return null;
        }
    }


}
