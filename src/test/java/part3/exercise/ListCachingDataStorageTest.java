package part3.exercise;

import data.Employee;
import data.JobHistoryEntry;
import data.Person;
import db.SlowCompletableFutureDb;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import part2.cache.CachingDataStorageImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class ListCachingDataStorageTest {
    private static final int MAX_CACHE_TIMEOUT = 1000;
    private static final int EMPLOYEE_CACHE_TIMEOUT = 300;
    private static SlowCompletableFutureDb<Employee> employeeDb;
    private static ListCachingDataStorage<String, Employee> storage;

    @BeforeClass
    public static void initializeDb() {
        employeeDb = new SlowCompletableFutureDb<>(new HashMap<>(), 1, TimeUnit.MILLISECONDS);
    }

    @AfterClass
    public static void closeDb() {
        try {
            employeeDb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void resetCaches() {
        CachingDataStorageImpl<Employee> employeeCache =
                new CachingDataStorageImpl<>(employeeDb, EMPLOYEE_CACHE_TIMEOUT, TimeUnit.MILLISECONDS);
        storage = new ListCachingDataStorage<>(employeeCache);
    }

    @Test
    public void getOutdatable() throws InterruptedException {
        final Person person1 = new Person("John", "Galt", 66);
        final Person person2 = new Person("Jane", "Doe", 66);
        final ArrayList<JobHistoryEntry> jobHistoryEntries = null;
        final Map<String, Employee> persons = new HashMap<>();

        persons.put(person1.toString(), new Employee(person1, jobHistoryEntries));
        persons.put(person2.toString(), new Employee(person2, jobHistoryEntries));
        employeeDb.setValues(persons);

        final ArrayList<String> keyList = new ArrayList<>(persons.keySet());
        final CompletableFuture<Void> future = storage.getOutdatable(keyList).getOutdated();
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