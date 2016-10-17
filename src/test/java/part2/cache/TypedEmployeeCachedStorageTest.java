package part2.cache;

import data.Employee;
import data.JobHistoryEntry;
import data.Person;
import data.typed.Employer;
import data.typed.Position;
import db.SlowCompletableFutureDb;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class TypedEmployeeCachedStorageTest {
    private static SlowCompletableFutureDb<Employee> employeeDb;
    private static SlowCompletableFutureDb<Employer> employerDb;
    private static SlowCompletableFutureDb<Position> positionDb;

    private static final int MAX_CACHE_TIMEOUT = 1000;
    private static final int EMPLOYEE_CACHE_TIMEOUT = 300;
    private static final int EMPLOYER_CACHE_TIMEOUT = 200;
    private static final int POSITION_CACHE_TIMEOUT = 100;
    private static final Person person = new Person("John", "Galt", 66);

    private static TypedEmployeeCachedStorage typedCache;

    private static ArrayList<JobHistoryEntry> jobHistoryEntries = null;

    @BeforeClass
    public static void initializeDb() {
        final Map<String, Employer> employerMap =
                Arrays.stream(Employer.values())
                        .collect(toMap(Employer::name, Function.identity()));
        employerDb = new SlowCompletableFutureDb<>(employerMap, 1, TimeUnit.MILLISECONDS);

        final Map<String, Position> positionMap =
                Arrays.stream(Position.values())
                        .collect(toMap(Position::name, Function.identity()));
        positionDb = new SlowCompletableFutureDb<>(positionMap, 1, TimeUnit.MILLISECONDS);

        employeeDb = new SlowCompletableFutureDb<>(new HashMap<>(), 1, TimeUnit.MILLISECONDS);

        final JobHistoryEntry jobHistoryEntry1 = new JobHistoryEntry(3, Position.DEV.toString(), Employer.EPAM.toString());
        final JobHistoryEntry jobHistoryEntry2 = new JobHistoryEntry(1, Position.QA.toString(), Employer.EPAM.toString());
        jobHistoryEntries = new ArrayList<>(Arrays.asList(jobHistoryEntry1, jobHistoryEntry2));

    }

    @AfterClass
    public static void closeDb() {
        try {
            employerDb.close();
            positionDb.close();
            employeeDb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void resetCaches() {
        CachingDataStorageImpl<Employee> employeeCache =
                new CachingDataStorageImpl<>(employeeDb, EMPLOYEE_CACHE_TIMEOUT, TimeUnit.MILLISECONDS);
        CachingDataStorageImpl<Employer> employerCache =
                new CachingDataStorageImpl<>(employerDb, EMPLOYER_CACHE_TIMEOUT, TimeUnit.MILLISECONDS);
        CachingDataStorageImpl<Position> positionCache =
                new CachingDataStorageImpl<>(positionDb, POSITION_CACHE_TIMEOUT, TimeUnit.MILLISECONDS);

        typedCache = new TypedEmployeeCachedStorage(employeeCache, positionCache, employerCache);
    }

    @Test
    public void outdatedWithJobHistoryLessThanCacheTimeout() throws InterruptedException {
        final Map<String, Employee> persons = new HashMap<>();
        persons.put(person.toString(), new Employee(person, jobHistoryEntries));
        employeeDb.setValues(persons);

        final CompletableFuture<Void> future = typedCache.getOutdatable(person.toString()).getOutdated();
        assertFalse(future.isDone());

        final long startTime = System.currentTimeMillis();
        long estimatedTime;
        do {
            Thread.sleep(10);
            estimatedTime = System.currentTimeMillis() - startTime;
        } while (!future.isDone() && estimatedTime < MAX_CACHE_TIMEOUT);
        System.out.printf("Estimated time (ms) = %d", estimatedTime);

        assertTrue(estimatedTime < POSITION_CACHE_TIMEOUT*2);
        assertTrue(future.isDone());
    }

    @Test
    public void outdatedWithoutJobHistoryLessThanCacheTimeout() throws InterruptedException {
        final Map<String, Employee> persons = new HashMap<>();
        persons.put(person.toString(), new Employee(person, Collections.emptyList()));
        employeeDb.setValues(persons);

        final CompletableFuture<Void> future = typedCache.getOutdatable(person.toString()).getOutdated();
        assertFalse(future.isDone());

        final long startTime = System.currentTimeMillis();
        long estimatedTime;
        do {
            Thread.sleep(10);
            estimatedTime = System.currentTimeMillis() - startTime;
        } while (!future.isDone() && estimatedTime < MAX_CACHE_TIMEOUT);
        System.out.printf("Estimated time (ms) = %d", estimatedTime);

        assertTrue(estimatedTime < EMPLOYEE_CACHE_TIMEOUT*2);
        assertTrue(future.isDone());
    }

}
