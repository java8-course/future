package part3.example;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

public class Methods {

    @Test
    public void methods() {
        final CompletableFuture<Integer> future = null;
        final CompletableFuture<Integer> future1 = null;
        final CompletableFuture<String> future2 = null;

        final CompletableFuture<Void> voidFuture = future.thenAccept(System.out::println);
        final CompletableFuture<Integer> mapped = future.thenApply(i -> i + 1);
        final CompletableFuture<Integer> combined =
                future.thenCompose(i -> CompletableFuture.completedFuture(i + 1));

        voidFuture.thenAccept(x -> System.out.println("completed"));
        voidFuture.thenRun(() -> System.out.println("completed"));

        final CompletableFuture<Void> allOf = CompletableFuture.allOf(future, future2);
        final CompletableFuture<Object> anyOf = CompletableFuture.anyOf(future, future);

        // any of
        future.acceptEither(future1, i -> System.out.println(i + 1));
        final CompletableFuture<Integer> r2 = future.applyToEither(future1, i -> i + 1);
        //future.runAfterEither();

        // all of
        //future.thenCombine(future2, (i, s) -> );
        //future.runAfterBoth();
        //future.thenAcceptBoth()

    }

}
