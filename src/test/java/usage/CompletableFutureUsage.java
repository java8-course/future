package usage;

import data.Employee;
import data.Generator;
import data.Person;
import db.SlowCompletableFutureDb;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class CompletableFutureUsage {

    private static SlowCompletableFutureDb<Employee> employeeDb;
    private static List<String> keys;

    @BeforeClass
    public static void before() {
        final Map<String, Employee> employeeMap = Generator.generateEmployeeList(1000)
                .stream()
                .collect(toMap(
                        e -> e.getPerson().getFirstName() + "_" + e.getPerson().getLastName() + "_" + e.getPerson().getAge(),
                        Function.identity(),
                        (a, b) -> a));
        employeeDb = new SlowCompletableFutureDb<>(employeeMap);

        keys = employeeMap.keySet().stream().collect(toList());
    }

    @AfterClass
    public static void after() {
        try {
            employeeDb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void consume() {
        final CompletableFuture<Employee> employeeCompletableFuture = employeeDb.get(keys.get(0));
        employeeCompletableFuture
                .thenAccept(System.out::println);
        final CompletableFuture<Person> personCompletableFuture =
                employeeCompletableFuture.thenApply(Employee::getPerson);

        final CompletableFuture<Employee> employeeCompletableFuture1 = employeeDb.get(keys.get(1));

        final CompletableFuture<Employee> employeeCompletableFuture2 =
                personCompletableFuture.thenCombine(
                        employeeCompletableFuture1,
                        (p, e) -> e.withPerson(p));

        final CompletableFuture<Integer> integerCompletableFuture =
                CompletableFuture.completedFuture(1);

        final CompletableFuture<String> stringCompletableFuture = new CompletableFuture<>();

        final CompletableFuture<Object> objectCompletableFuture = CompletableFuture.anyOf(
                new CompletableFuture[]{employeeCompletableFuture, employeeCompletableFuture1});
    }
}
