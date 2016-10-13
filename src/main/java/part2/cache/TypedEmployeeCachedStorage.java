package part2.cache;

import data.Person;
import data.typed.Employee;
import data.typed.Employer;
import data.typed.JobHistoryEntry;
import data.typed.Position;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class TypedEmployeeCachedStorage implements CachingDataStorage<String, data.typed.Employee> {

    private final CachingDataStorage<String, data.Employee> employeeStorage;
    private final CachingDataStorage<String, Position> positionStorage;
    private final CachingDataStorage<String, Employer> employerStorage;

    public TypedEmployeeCachedStorage(CachingDataStorage<String, data.Employee> employeeStorage,
                                      CachingDataStorage<String, Position> positionStorage,
                                      CachingDataStorage<String, Employer> employerStorage) {
        this.employeeStorage = employeeStorage;
        this.positionStorage = positionStorage;
        this.employerStorage = employerStorage;
    }

    private static class TypedEmployeeBuilder {
        final int historySize;
        final Position[] positions;
        final Employer[] employers;
        final int[] durations;
        final AtomicInteger entriesRequired;
        private final Person person;

        // Gets completed when all positions and employers are filled
        private final CompletableFuture<Employee> futureEmployee;

        private TypedEmployeeBuilder(data.Employee employee) {
            historySize = employee.getJobHistory().size();
            positions = new Position[historySize];
            employers = new Employer[historySize];
            durations = new int[historySize];
            entriesRequired = new AtomicInteger(historySize * 2);
            person = employee.getPerson();
            futureEmployee = new CompletableFuture<>();
        }

        void setDuration(int i, int duration) {
            durations[i] = duration;
        }

        void setPosition(int i, Position pos) {
            positions[i] = pos;
            if (entriesRequired.decrementAndGet() == 0) completeFuture();
        }

        void setEmployer(int i, Employer emplr) {
            employers[i] = emplr;
            if (entriesRequired.decrementAndGet() == 0) completeFuture();
        }

        private void completeFuture() {
            List<JobHistoryEntry> jobs = new ArrayList<>(historySize);
            for (int i = 0; i < historySize; i++)
                jobs.add(new JobHistoryEntry(positions[i], employers[i], durations[i]));
            futureEmployee.complete(new Employee(person, jobs));
        }

        CompletableFuture<Employee> getFutureEmployee() {
            return futureEmployee;
        }
    }

    private CompletableFuture<Employee> constructTypedEmployee(data.Employee employee, Runnable whenOutdated) {
        final List<data.JobHistoryEntry> jobHistory = employee.getJobHistory();

        final TypedEmployeeBuilder builder = new TypedEmployeeBuilder(employee);

        // Getting list elements by index may be slow,
        // forEach cannot guarantee proper order of the elements,
        // have to use "for" loop with iterator
        final ListIterator<data.JobHistoryEntry> jobIterator = jobHistory.listIterator();
        for (int i = 0; i < jobHistory.size(); i++) {
            final int index = i;
            final data.JobHistoryEntry jobHistoryEntry = jobIterator.next();
            builder.setDuration(index, jobHistoryEntry.getDuration());
            final OutdatableResult<Employer> employer = employerStorage.getOutdatable(jobHistoryEntry.getEmployer());
            employer.getResult().thenAccept(emplr -> builder.setEmployer(index, emplr));
            employer.getOutdated().thenRun(whenOutdated);
            final OutdatableResult<Position> position = positionStorage.getOutdatable(jobHistoryEntry.getPosition());
            position.getResult().thenAccept(pos -> builder.setPosition(index, pos));
            position.getOutdated().thenRun(whenOutdated);
        }

        return builder.getFutureEmployee();
    }

    @Override
    public OutdatableResult<Employee> getOutdatable(String key) {
        CompletableFuture<Employee> typedEmployee = new CompletableFuture<>();
        CompletableFuture<Void> typedOutdated = new CompletableFuture<>();

        final OutdatableResult<data.Employee> untypedEmployee = employeeStorage.getOutdatable(key);

        final Runnable outdatedAction = () -> typedOutdated.complete(null);

        untypedEmployee.getOutdated().thenRun(outdatedAction);

        untypedEmployee.getResult().thenAccept(empl -> {
            final CompletableFuture<Employee> futureEmployee = constructTypedEmployee(empl, outdatedAction);
            futureEmployee.thenAccept(typedEmployee::complete);
        });

        return new OutdatableResult<>(typedEmployee, typedOutdated);
    }
}
