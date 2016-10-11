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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

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
    public void test() {
        final String key = keys.get(0);
        final CompletableFuture<Employee> employeeCompletableFuture =
                employeeDb.get(key);

        // M<T>:
        // point: T -> M<T>
        // flatMap: M<T> -> (T -> M<R>) -> M<R>
        // map: M<T> -> (T -> R) -> M<R>

        // Optional<T>
        // point: T -> M<T>
        final int t1 = 1;
        final Optional<Integer> optT1 = Optional.of(t1);
        // flatMap: M<T> -> (T -> M<R>) -> M<R>
        final Function<Integer, Optional<String>> fo = i -> i > 0 ? Optional.of(i.toString()) : Optional.empty();
        final Optional<String> stringOptional = optT1.flatMap(fo);
        // map: M<T> -> (T -> R) -> M<R>
        final Function<Integer, String> f = Object::toString;
        final Optional<String> stringOptional1 = optT1.map(f);

        // point: T -> M<T>
        final Stream<Integer> stream = Stream.of(t1);
        // flatMap: M<T> -> (T -> M<R>) -> M<R>
        final Function<Integer, Stream<Double>> fs = i -> i.toString().chars().mapToDouble(x -> x).boxed();
        final Stream<Double> doubleStream = stream.flatMap(fs);
        // map: M<T> -> (T -> R) -> M<R>
        stream.map(f);


        // M<T>:
        // point: T -> M<T>
        final CompletableFuture<Integer> integerCompletableFuture = CompletableFuture.completedFuture(t1);
        // flatMap: M<T> -> (T -> M<R>) -> M<R>
        final Function<Integer, CompletableFuture<String>> fcf = i -> CompletableFuture.completedFuture(i.toString());
        final CompletableFuture<CompletableFuture<String>> completableFutureCompletableFuture = integerCompletableFuture.thenApply(fcf);

        // source   *---------------------!==========
        // lambda                         *------!====
        // result            *-------------------!===
        final CompletableFuture<String> stringCompletableFuture1 = integerCompletableFuture.thenCompose(fcf);

        // map: M<T> -> (T -> R) -> M<R>

        // source    *--------------------!============
        //                              (lambda)
        // result            *------------!============
        final CompletableFuture<String> stringCompletableFuture = integerCompletableFuture.thenApply(f);


    }

    @Test
    public void consume() {
        final CompletableFuture<Employee> employeeCompletableFuture =
                employeeDb.get(keys.get(0));


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
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stringCompletableFuture.complete("result!");
        }).start();


        final CompletableFuture<Object> objectCompletableFuture = CompletableFuture.anyOf(
                new CompletableFuture[]{employeeCompletableFuture, employeeCompletableFuture1});
    }
}
