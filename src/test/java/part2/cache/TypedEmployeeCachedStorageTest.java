package part2.cache;

import data.Employee;
import data.Generator;
import data.JobHistoryEntry;
import data.Person;
import data.typed.Employer;
import data.typed.Position;
import db.SlowCompletableFutureDb;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

public class TypedEmployeeCachedStorageTest {
    private static SlowCompletableFutureDb<Employee> employeeDb;
    private static SlowCompletableFutureDb<Employer> employerDb;
    private static SlowCompletableFutureDb<Position> positionDb;

    @BeforeClass
    public static void defore() {
        final Map<String, Employer> employerMap =
                Arrays.stream(Employer.values())
                        .collect(toMap(Employer::name, Function.identity()));
        employerDb = new SlowCompletableFutureDb<>(employerMap, 1, TimeUnit.MILLISECONDS);

        final Map<String, Position> positionMap =
                Arrays.stream(Position.values())
                        .collect(toMap(Position::name, Function.identity()));
        positionDb = new SlowCompletableFutureDb<>(positionMap, 1, TimeUnit.MILLISECONDS);

        employeeDb = new SlowCompletableFutureDb<>(new HashMap<>(), 1, TimeUnit.MILLISECONDS);
    }

    @AfterClass
    public static void after() {
        try {
            employerDb.close();
            positionDb.close();
            employeeDb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void expiration() throws InterruptedException {
        final CachingDataStorageImpl<Employee> employeeCache =
                new CachingDataStorageImpl<>(employeeDb, 1, TimeUnit.SECONDS);

        final CachingDataStorageImpl<Employer> employerCache =
                new CachingDataStorageImpl<>(employerDb, 2, TimeUnit.SECONDS);

        final CachingDataStorageImpl<Position> positionCache =
                new CachingDataStorageImpl<>(positionDb, 100, TimeUnit.MILLISECONDS);

        final TypedEmployeeCachedStorage typedCache =
                new TypedEmployeeCachedStorage(employeeCache, positionCache, employerCache);

        // TODO check than cache gets outdated with the firs outdated inner cache
        final Map<String, data.Employee> employeeMap =
                Generator.generateEmployeeList(10).stream()
                        .collect(Collectors.toMap((data.Employee e) -> e.getPerson().toString(),Function.identity()));
        employeeDb.setValues(employeeMap);

        final Iterator<String> untypedKeys = employeeMap.keySet().iterator();
        final String key = untypedKeys.next();
        final Employee untypedEmployee = employeeMap.get(key);
        final data.typed.Employee typedEmployee = typedCache.get(key).join();
        Assert.assertEquals(untypedEmployee.getPerson(),typedEmployee.getPerson());
        final Iterator<data.typed.JobHistoryEntry> typedJobHistory = typedEmployee.getJobHistoryEntries().iterator();
        untypedEmployee.getJobHistory().stream()
                .forEach(jhe -> {
                    data.typed.JobHistoryEntry typedEntry = typedJobHistory.next();
                    Assert.assertEquals(jhe.getDuration(),typedEntry.getDuration());
                    Assert.assertEquals(jhe.getEmployer(),typedEntry.getEmployer().toString());
                    Assert.assertEquals(jhe.getPosition(),typedEntry.getPosition().toString());
                });
        String key2 = untypedKeys.next();
        while (employeeMap.get(key2).getJobHistory().isEmpty()){
            key2 = untypedKeys.next();
        }
        final CompletableFuture<Void> outdated = typedCache.getOutdatable(key2).getOutdated();
        Assert.assertFalse(outdated.isDone());
        Thread.sleep(50);
        Assert.assertFalse(outdated.isDone());
        Thread.sleep(60);
        Assert.assertTrue(outdated.isDone());
    }
}
