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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import part3.exercise.ComposeCachingDataStorage;
import part3.exercise.ListCachingDataStorage;
import part3.exercise.MappingCachingDataStorage;
import part3.exercise.PairCachingDataStorage;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class TypedEmployeeCachedStorageTest {
    private static SlowCompletableFutureDb<Employee> employeeDb;
    private static SlowCompletableFutureDb<Employer> employerDb;
    private static SlowCompletableFutureDb<Position> positionDb;

    private enum TestMode {CLASSIC, FUNCTIONAL}

    @Parameterized.Parameters
    public static Collection<TestMode> testModes() {
        return Arrays.asList(TestMode.values());
    }

    private final CachingDataStorage<String, data.typed.Employee> typedCache;
    private final TestMode currentMode;

    public TypedEmployeeCachedStorageTest(TestMode testMode) {
        final CachingDataStorageImpl<Employee> employeeCache =
                new CachingDataStorageImpl<>(employeeDb, 1, TimeUnit.SECONDS);

        final CachingDataStorageImpl<Employer> employerCache =
                new CachingDataStorageImpl<>(employerDb, 2, TimeUnit.SECONDS);

        final CachingDataStorageImpl<Position> positionCache =
                new CachingDataStorageImpl<>(positionDb, 100, TimeUnit.MILLISECONDS);

        switch (testMode) {
            case CLASSIC:
                typedCache =
                        new TypedEmployeeCachedStorage(employeeCache, positionCache, employerCache);
                break;
            case FUNCTIONAL:
                final CachingDataStorage<JobHistoryEntry, data.typed.JobHistoryEntry> JobHistoryEntryCache =
                        new PairCachingDataStorage<>(employerCache, positionCache, JobHistoryEntry::getEmployer, JobHistoryEntry::getPosition,
                                jobHistoryEntry -> (employer, position) -> new data.typed.JobHistoryEntry(position, employer, jobHistoryEntry.getDuration()));

                final CachingDataStorage<List<JobHistoryEntry>, List<data.typed.JobHistoryEntry>> JobHistoryListCache =
                        new ListCachingDataStorage<>(JobHistoryEntryCache);

                final CachingDataStorage<Employee, data.typed.Employee> employeeToTypedCache =
                        new MappingCachingDataStorage<>(JobHistoryListCache, Employee::getJobHistory, (e, jl) -> new data.typed.Employee(e.getPerson(), jl));

                typedCache = new ComposeCachingDataStorage<>(employeeCache, employeeToTypedCache, Function.identity());
                break;
            default:
                throw new IllegalArgumentException("Unsupported test mode");
        }
        currentMode = testMode;
    }

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

    private Person johnGalt37 = new Person("John", "Galt", 37);
    private JobHistoryEntry jobDevEpam = new JobHistoryEntry(3, "DEV", "EPAM");
    private JobHistoryEntry jobQAGoogle = new JobHistoryEntry(2, "QA", "Google");
    private List<JobHistoryEntry> twoJobs = new ArrayList<>(Arrays.asList(jobDevEpam, jobQAGoogle));

    private data.typed.JobHistoryEntry jobDevEpamT = new data.typed.JobHistoryEntry(Position.DEV, Employer.EPAM, 3);
    private data.typed.JobHistoryEntry jobQAGoogleT = new data.typed.JobHistoryEntry(Position.QA, Employer.Google, 2);
    private List<data.typed.JobHistoryEntry> twoJobsT = new ArrayList<>(Arrays.asList(jobDevEpamT, jobQAGoogleT));

    @Before
    public void setupEmployeeDB() {
        final HashMap<String, Employee> untypedEmployees = new HashMap<>();

        untypedEmployees.put("a", new Employee(johnGalt37, twoJobs));

        employeeDb.setValues(untypedEmployees);
    }

    private void printTimeStamp(String message, long relativeTo) {
        System.out.println(message + (System.currentTimeMillis() - relativeTo));
    }

    @Test
    public void testGetTypedEmployee() throws InterruptedException, ExecutionException {
        System.out.println("Test mode: " + currentMode);
        long startTime = System.currentTimeMillis();
        printTimeStamp("Start: ", startTime);
        final CachingDataStorage.OutdatableResult<data.typed.Employee> empA = typedCache.getOutdatable("a");
        printTimeStamp("GetOutdatable returned: ", startTime);

        assertThat("Outdated too soon", empA.getOutdated().isDone(), is(false));

        Thread.sleep(25);
        printTimeStamp("After 25 ms sleep: ", startTime);

        final data.typed.Employee expected = new data.typed.Employee(johnGalt37, twoJobsT);

        assertThat("Not done", empA.getResult().isDone(), is(true));
        assertThat("Wrong result", empA.getResult().get(), is(expected));
        assertThat("Outdated too soon", empA.getOutdated().isDone(), is(false));
        printTimeStamp("After assertions: ", startTime);

        for (int i = 0; i < 12; i++) {          // Experimentally determined outdation time: 70 to 110 ms
            if (empA.getOutdated().isDone()) {
                System.out.printf("Outdated after %d ms\n", i * 10);
                break;
            }
            Thread.sleep(10);
        }
        printTimeStamp("Final time: ", startTime);
        assertThat("Not outdated", empA.getOutdated().isDone(), is(true));
    }

}
