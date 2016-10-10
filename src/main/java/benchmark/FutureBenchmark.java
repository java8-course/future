package benchmark;

import data.Employee;
import data.Generator;
import data.typed.Employer;
import data.typed.JobHistoryEntry;
import data.typed.Position;
import db.SlowBlockingDb;
import db.SlowCompletableFutureDb;
import db.SlowFutureDb;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
//@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
//@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
public class FutureBenchmark {

    @Param({"400", "4000", "40000"})
    public int requestCount;

    @Param({"10000"})
    public int employeesCount;

    private SlowBlockingDb<Employer> blockingEmployers;
    private SlowBlockingDb<Position> blockingPositions;
    private SlowBlockingDb<Employee> blockingEmployee;
    private List<String> requests;
    private ExecutorService blockingExecutorService;


    @Setup
    public void setup() {
        final Map<String, Employer> employerMap =
                Arrays.stream(Employer.values())
                        .collect(toMap(Employer::name, Function.identity()));
        blockingEmployers = new SlowBlockingDb<>(employerMap);

        final Map<String, Position> positionMap =
                Arrays.stream(Position.values())
                        .collect(toMap(Position::name, Function.identity()));
        blockingPositions = new SlowBlockingDb<>(positionMap);

        final Map<String, Employee> employeeMap =
                Generator.generateEmployeeList(employeesCount).stream()
                        .collect(toMap(
                                e -> e.getPerson().getFirstName() + "_" + e.getPerson().getLastName() + "_" + e.getPerson().getAge(),
                                Function.identity(),
                                (e1, e2) -> e1
                        ));

        blockingEmployee = new SlowBlockingDb<>(employeeMap);

        final String[] keys = employeeMap.keySet().stream().toArray(String[]::new);

        requests = Stream.generate(() -> keys[ThreadLocalRandom.current().nextInt(keys.length)])
                .limit(requestCount * 10)
                .distinct()
                .limit(requestCount)
                .collect(toList());

        blockingExecutorService = Executors.newCachedThreadPool();
    }

    @TearDown
    public void tearDown() {
        blockingExecutorService.shutdown();
        blockingExecutorService.shutdownNow();
        try {
            blockingExecutorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            blockingPositions.close();
            blockingEmployers.close();
            blockingEmployee.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    public void blockingProcessing(Blackhole bh) {
        final List<Future<?>> futures = requests.stream()
                .map(
                        r -> blockingExecutorService.submit(() -> {
                            final data.typed.Employee employee = blockingGetTypedEmployee(r);
                            bh.consume(employee);
                        })
                )
                .collect(toList());

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private Position blockingGetPosition(String key) {
        try {
            return blockingPositions.get(key);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Employer blockingGetEmployer(String key) {
        try {
            return blockingEmployers.get(key);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    private data.typed.Employee blockingGetTypedEmployee(String key) {
        try {
            final Employee employee = blockingEmployee.get(key);

            final List<JobHistoryEntry> jobHistoryEntries = employee.getJobHistory().stream()
                    .map(j ->
                            new JobHistoryEntry(
                                    blockingGetPosition(j.getPosition()),
                                    blockingGetEmployer(j.getEmployer()),
                                    j.getDuration()))
                    .collect(toList());

            return new data.typed.Employee(employee.getPerson(), jobHistoryEntries);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Benchmark
    public void futureProcessing(Blackhole bh) {
        final List<Future<?>> futures = requests.stream()
                .map(
                        r -> blockingExecutorService.submit(() -> {
                            final data.typed.Employee employee = futureGetTypedEmployee(r);
                            bh.consume(employee);
                        })
                )
                .collect(toList());

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private data.typed.Employee futureGetTypedEmployee(String key) {
        try {
            final Employee employee = blockingEmployee.get(key);

            final Map<String, Future<Employer>> employers = new HashMap<>();
            final Map<String, Future<Position>> positions = new HashMap<>();

            final SlowFutureDb<Employer> futureEmployersDb = blockingEmployers.getFutureDb();
            final SlowFutureDb<Position> futurePositionDb = blockingPositions.getFutureDb();

            for (data.JobHistoryEntry j : employee.getJobHistory()) {
                employers.put(j.getEmployer(), futureEmployersDb.get(j.getEmployer()));
                positions.put(j.getPosition(), futurePositionDb.get(j.getPosition()));
            }

            final List<JobHistoryEntry> jobHistoryEntries =
                    employee.getJobHistory().stream()
                            .map(j ->
                                    new JobHistoryEntry(
                                            getOrNull(positions.get(j.getPosition())),
                                            getOrNull(employers.get(j.getEmployer())),
                                            j.getDuration()))
                            .collect(toList());

            return new data.typed.Employee(employee.getPerson(), jobHistoryEntries);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Benchmark
    public void completableProcessing(Blackhole bh) {
        final List<Future<?>> futures = requests.stream()
                .map(r -> completableFutureGetTypedEmployee(r)
                        .thenAccept(bh::consume))
                .collect(toList());

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private CompletableFuture<data.typed.Employee> completableFutureGetTypedEmployee(String key) {
        final SlowCompletableFutureDb<Employee> employeeDb = blockingEmployee.getFutureDb().getCompletableFutureDb();

        final CompletableFuture<Employee> employee = employeeDb.get(key);

        return employee.thenComposeAsync((e) -> asyncToTyped(e));

    }

    private CompletionStage<data.typed.Employee> asyncToTyped(Employee e) {
        final List<CompletableFuture<JobHistoryEntry>> jobHistoryFutures =
                e.getJobHistory().stream()
                        .map(this::asyncToTyped)
                        .collect(toList());

        return CompletableFuture.allOf(jobHistoryFutures.toArray(new CompletableFuture[0]))
                .thenApplyAsync(x -> {
                    final List<JobHistoryEntry> jobHistory = jobHistoryFutures.stream()
                            .map(FutureBenchmark::getOrNull)
                            .collect(toList());

                    return new data.typed.Employee(e.getPerson(), jobHistory);
                });
    }

    private CompletableFuture<JobHistoryEntry> asyncToTyped(data.JobHistoryEntry j) {
        final SlowCompletableFutureDb<Employer> employersDb = blockingEmployers.getFutureDb().getCompletableFutureDb();
        final SlowCompletableFutureDb<Position> positionDb = blockingPositions.getFutureDb().getCompletableFutureDb();

        return employersDb.get(j.getEmployer())
                .thenCombine(
                        positionDb.get(j.getPosition()),
                        (e, p) -> new JobHistoryEntry(p, e, j.getDuration()));
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
