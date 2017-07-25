package part1.exercise;

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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class CompletableFutureBasics {

    private static SlowCompletableFutureDb<Employee> employeeDb;
    private static List<String> keys;

    @BeforeClass
    public static void before() {
        final Map<String, Employee> employeeMap = Generator.generateEmployeeList(1000)
                .stream()
                .collect(toMap(
                        e -> getKeyByPerson(e.getPerson()),
                        Function.identity(),
                        (a, b) -> a));
        employeeDb = new SlowCompletableFutureDb<>(employeeMap);

        keys = employeeMap.keySet().stream().collect(toList());
    }

    private static String getKeyByPerson(Person person) {
        return person.getFirstName() + "_" + person.getLastName() + "_" + person.getAge();
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
    public void createNonEmpty() throws ExecutionException, InterruptedException {
        final Person person = new Person("John", "Galt", 33);

        final Optional<Person> optPerson = Optional.of(person);

        assertTrue(optPerson.isPresent());
        assertEquals(person, optPerson.get());

        final Stream<Person> streamPerson = Stream.of(person);

        final List<Person> persons = streamPerson.collect(toList());
        assertThat(persons.size(), is(1));
        assertEquals(person, persons.get(0));

        final CompletableFuture<Person> futurePerson = CompletableFuture.completedFuture(person);

        assertTrue(futurePerson.isDone());
        assertEquals(person, futurePerson.get());
    }

    @Test
    public void createEmpty() throws ExecutionException, InterruptedException {

        final Optional<Person> optPerson = Optional.empty();

        assertFalse(optPerson.isPresent());


        final Stream<Person> streamPerson = Stream.empty();

        final List<Person> persons = streamPerson.collect(toList());
        assertThat(persons.size(), is(0));

        final CompletableFuture<Person> futurePerson = new CompletableFuture<>();
        futurePerson.completeExceptionally(new NoSuchElementException());

        assertTrue(futurePerson.isCompletedExceptionally());
        assertTrue(futurePerson
                .thenApply(x -> false)
                .exceptionally(t -> t.getCause() instanceof NoSuchElementException).get());
    }

    @Test
    public void forEach() throws ExecutionException, InterruptedException {
        final Person person = new Person("John", "Galt", 33);

        final Optional<Person> optPerson = Optional.of(person);

        final CompletableFuture<Person> result1 = new CompletableFuture<>();

        optPerson.ifPresent(result1::complete);

        assertEquals(person, result1.get());

        final Stream<Person> streamPerson = Stream.of(person);

        final CompletableFuture<Person> result2 = new CompletableFuture<>();

        streamPerson.forEach(result2::complete);
        assertEquals(person, result2.get());

        final CompletableFuture<Person> futurePerson = CompletableFuture.completedFuture(person);

        final CompletableFuture<Person> result3 = new CompletableFuture<>();

        futurePerson.thenAccept(result3::complete);

        assertEquals(person, result3.get());
    }

    @Test
    public void map() throws ExecutionException, InterruptedException {
        final Person person = new Person("John", "Galt", 33);

        final Optional<Person> optPerson = Optional.of(person);

        final Optional<String> optFirstName = optPerson.map(Person::getFirstName);

        assertEquals(person.getFirstName(), optFirstName.get());

        final Stream<Person> streamPerson = Stream.of(person);

        final Stream<String> streamFirstName = streamPerson.map(Person::getFirstName);

        assertEquals(person.getFirstName(), streamFirstName.collect(toList()).get(0));

        final CompletableFuture<Person> futurePerson = CompletableFuture.completedFuture(person);

        final CompletableFuture<String> futureFirstName = futurePerson.thenApply(Person::getFirstName);

        assertEquals(person.getFirstName(), futureFirstName.get());
    }

    @Test
    public void flatMap() throws ExecutionException, InterruptedException {
        final Person person = employeeDb.get(keys.get(0)).thenApply(Employee::getPerson).get();

        final Optional<Person> optPerson = Optional.of(person);

        final Optional<Integer> optFirstCodePointOfFirstName = optPerson
                .flatMap(p -> p.getFirstName().codePoints().mapToObj(p1 -> p1).findFirst());

        assertEquals(Integer.valueOf(65), optFirstCodePointOfFirstName.get());

        final Stream<Person> streamPerson = Stream.of(person);

        final IntStream codePoints = streamPerson.flatMapToInt(p -> p.getFirstName().codePoints());

        final int[] codePointsArray = codePoints.toArray();
        assertEquals(person.getFirstName(), new String(codePointsArray, 0, codePointsArray.length));

        final CompletableFuture<Person> futurePerson = CompletableFuture.completedFuture(person);

        final CompletableFuture<Employee> futureEmployee = futurePerson.thenCompose(p -> employeeDb.get(getKeyByPerson(p)));

        assertEquals(person, futureEmployee.thenApply(Employee::getPerson).get());
    }
}
