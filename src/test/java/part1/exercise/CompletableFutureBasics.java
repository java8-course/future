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

        // TODO Create non empty Optional
        final Optional<Person> optPerson = Optional.of(person);

        assertTrue(optPerson.isPresent());
        assertEquals(person, optPerson.get());

        // TODO Create stream with a single element
        final Stream<Person> streamPerson = Stream.of(person);

        final List<Person> persons = streamPerson.collect(toList());
        assertThat(persons.size(), is(1));
        assertEquals(person, persons.get(0));

        // TODO Create completed CompletableFuture
        final CompletableFuture<Person> futurePerson = CompletableFuture.completedFuture(person);

        assertTrue(futurePerson.isDone());
        assertEquals(person, futurePerson.get());
    }

    @Test
    public void createEmpty() throws ExecutionException, InterruptedException {
        // TODO Create empty Optional
        final Optional<Person> optPerson = Optional.empty();

        assertFalse(optPerson.isPresent());

        // TODO Create empty stream
        final Stream<Person> streamPerson = Stream.empty();

        final List<Person> persons = streamPerson.collect(toList());
        assertThat(persons.size(), is(0));

        // TODO Complete CompletableFuture with NoSuchElementException
        final CompletableFuture<Person> futurePerson = new CompletableFuture<>();
        futurePerson.obtrudeException(new NoSuchElementException());
        // futurePerson.???

        assertTrue(futurePerson.isCompletedExceptionally());
        assertTrue(futurePerson
                .thenApply(x -> false)
                .exceptionally(t -> t.getCause() instanceof NoSuchElementException).get());
    }

    @Test
    public void forEach() throws ExecutionException, InterruptedException {
        final Person person = new Person("John", "Galt", 33);

        // TODO Create non empty Optional
        final Optional<Person> optPerson = Optional.of(person);

        final CompletableFuture<Person> result1 = new CompletableFuture<>();

        // TODO using optPerson.ifPresent complete result1
        optPerson.ifPresent(person1 -> result1.complete(person1));
        assertEquals(person, result1.get());

        // TODO Create stream with a single element
        final Stream<Person> streamPerson = Stream.of(person);

        final CompletableFuture<Person> result2 = new CompletableFuture<>();

        // TODO Using streamPerson.forEach complete result2
        streamPerson.forEach(result2::complete);
        assertEquals(person, result2.get());

        // TODO Create completed CompletableFuture
        final CompletableFuture<Person> futurePerson = CompletableFuture.completedFuture(person);

        final CompletableFuture<Person> result3 = new CompletableFuture<>();

        // TODO Using futurePerson.thenAccept complete result3
        futurePerson.thenAccept(result3::complete);
        assertEquals(person, result3.get());
    }

    @Test
    public void map() throws ExecutionException, InterruptedException {
        final Person person = new Person("John", "Galt", 33);

        // TODO Create non empty Optional
        final Optional<Person> optPerson = Optional.of(person);

        // TODO get Optional<first name> from optPerson
        final Optional<String> optFirstName = optPerson.map(person1 -> person1.getFirstName());

        assertEquals(person.getFirstName(), optFirstName.get());

        // TODO Create stream with a single element
        final Stream<Person> streamPerson = Stream.of(person);

        // TODO Get Stream<first name> from streamPerson
        final Stream<String> streamFirstName = streamPerson.map(Person::getFirstName);

        assertEquals(person.getFirstName(), streamFirstName.collect(toList()).get(0));

        // TODO Create completed CompletableFuture
        final CompletableFuture<Person> futurePerson = CompletableFuture.completedFuture(person);

        // TODO Get CompletableFuture<first name> from futurePerson
        final CompletableFuture<String> futureFirstName = futurePerson.thenApply(Person::getFirstName);

        assertEquals(person.getFirstName(), futureFirstName.get());
    }

    @Test
    public void flatMap() throws ExecutionException, InterruptedException {
        final Person person = employeeDb.get(keys.get(0)).thenApply(Employee::getPerson).get();

        // TODO Create non empty Optional
        final Optional<Person> optPerson = Optional.of(person);

        // TODO Using flatMap and .getFirstName().codePoints().mapToObj(p -> p).findFirst()
        // TODO get the first letter of first name if any
        final Optional<Integer> optFirstCodePointOfFirstName =
                optPerson.flatMap(person1 -> person1.getFirstName().codePoints().mapToObj(value -> value).findFirst());

        assertEquals(Integer.valueOf(65), optFirstCodePointOfFirstName.get());

        // TODO Create stream with a single element
        final Stream<Person> streamPerson = Stream.of(person);

        // TODO Using flatMapToInt and .getFirstName().codePoints() get codepoints stream from streamPerson
        final IntStream codePoints = streamPerson.flatMapToInt(person1 -> person1.getFirstName().codePoints());

        final int[] codePointsArray = codePoints.toArray();
        assertEquals(person.getFirstName(), new String(codePointsArray, 0, codePointsArray.length));

        // TODO Create completed CompletableFuture
        final CompletableFuture<Person> futurePerson = CompletableFuture.completedFuture(person);

        // TODO Get CompletableFuture<Employee> from futurePerson using getKeyByPerson and employeeDb
        final CompletableFuture<Employee> futureEmployee = futurePerson.thenCompose(person1 -> employeeDb.get(getKeyByPerson(person1)));

        assertEquals(person, futureEmployee.thenApply(Employee::getPerson).get());
    }
}
