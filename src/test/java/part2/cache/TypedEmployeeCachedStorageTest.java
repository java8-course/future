package part2.cache;

import data.Employee;
import data.JobHistoryEntry;
import data.Person;
import data.typed.Employer;
import data.typed.Position;
import db.SlowCompletableFutureDb;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

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
    public void expiration() throws ExecutionException, InterruptedException {
        final CachingDataStorageImpl<Employee> employeeCache =
                new CachingDataStorageImpl<>(employeeDb, 1, TimeUnit.SECONDS);

        final CachingDataStorageImpl<Employer> employerCache =
                new CachingDataStorageImpl<>(employerDb, 2, TimeUnit.SECONDS);

        final CachingDataStorageImpl<Position> positionCache =
                new CachingDataStorageImpl<>(positionDb, 100, TimeUnit.MILLISECONDS);

        Map<String, Employee> employeeTmp = new HashMap<>();

        final Person person1 = new Person("John", "Doe", 30);
        employeeTmp.put("a", new Employee(person1,
                Collections.singletonList(new JobHistoryEntry(1, Position.BA.name(), Employer.EPAM.name()))));
        employeeDb.setValues(employeeTmp);

        final TypedEmployeeCachedStorage typedCache =
                new TypedEmployeeCachedStorage(employeeCache, positionCache, employerCache);

        final CachingDataStorage.OutdatableResult<data.typed.Employee> aPerson = typedCache.getOutdatable("a");

        assertEquals(aPerson.getResult().get().getPerson(), person1);
        assertEquals(aPerson.getResult().get().getJobHistoryEntries(),
                Collections.singletonList(new data.typed.JobHistoryEntry(Position.BA, Employer.EPAM, 1)));

        Thread.sleep(500);
        employeeTmp = new HashMap<>();
        final Person person2 = new Person("Dagni", "Taggart", 30);
        employeeTmp.put("a", new Employee(person2, Collections.emptyList()));
        employeeDb.setValues(employeeTmp);

        final CachingDataStorage.OutdatableResult<data.typed.Employee> aPerson2 = typedCache.getOutdatable("a");
        assertEquals(aPerson2.getResult().get().getPerson(), person1);

        Thread.sleep(500);
        final CachingDataStorage.OutdatableResult<data.typed.Employee> aPerson3 = typedCache.getOutdatable("a");
        assertEquals(aPerson3.getResult().get().getPerson(), person2);
    }
}
