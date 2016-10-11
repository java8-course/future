package part1.exercise;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Methods {

    private ForkJoinPool customExecutor = ForkJoinPool.commonPool();

    private Integer slowTask() {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return ThreadLocalRandom.current().nextInt();
    }

    @Test
    public void create() {
        // new
        // supplyAsync
        // runAsync
        // completed
    }

    @Test
    public void write() {
        // complete (completed)
        // completeExceptionally
    }

    @Test
    public void rewrite() {
        // obtrudeValue
        // obtrudeException
    }

    @Test
    public void read() {
        // isDone
        // get
        // join
        // getNow
        // getNumberOfDependents
    }

    @Test
    public void foreachMapFlatMap() {
        final CompletableFuture<Integer> future = null;
        final CompletableFuture<Integer> future1 = null;
        final CompletableFuture<String> future2 = null;

        // forEach
        // map
        // flatMap
        // *3
    }

    @Test
    public void allAnyOf() {
        final CompletableFuture<Integer> future = null;
        final CompletableFuture<Integer> future1 = null;
        final CompletableFuture<String> future2 = null;

        // All of
        // Any of

        // any of
        // acceptEither
        // applyToEither
        // runAfterEither

        // all of
        // thenCombine
        // runAfterBoth
        // thenAcceptBoth

    }

}
