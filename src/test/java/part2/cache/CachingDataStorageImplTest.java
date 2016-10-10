package part2.cache;

import data.Employee;
import data.Person;
import data.typed.Employer;
import data.typed.Position;
import db.SlowCompletableFutureDb;
import part2.cache.CachingDataStorage.OutdatableResult;
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
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

public class CachingDataStorageImplTest {
    private static SlowCompletableFutureDb<data.Employee> employeeDb;
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
    public void expiration() throws InterruptedException, ExecutionException, TimeoutException {
        final CachingDataStorageImpl<Employee> employeeCache =
                new CachingDataStorageImpl<>(employeeDb, 100, TimeUnit.MILLISECONDS);

        Map<String, Employee> values = new HashMap<>();
        final Person person1 = new Person("John", "Galt", 66);
        values.put("a", new Employee(person1, Collections.emptyList()));
        employeeDb.setValues(values);

        final OutdatableResult<Employee> result1 = employeeCache.getOutdatable("a");

        values = new HashMap<>();
        final Person person2 = new Person("John", "Doe", 30);
        values.put("a", new Employee(person2, Collections.emptyList()));
        employeeDb.setValues(values);

        Thread.sleep(10);
        final OutdatableResult<Employee> result2 = employeeCache.getOutdatable("a");

        assertEquals(person1, result1.getResult().get().getPerson());
        assertEquals(result1.getResult().get(), result2.getResult().get());

        result1.getOutdated().get(100, TimeUnit.MILLISECONDS);

        final OutdatableResult<Employee> result3 = employeeCache.getOutdatable("a");

        assertEquals(person2, result3.getResult().get().getPerson());
    }

}
