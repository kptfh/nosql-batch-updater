package nosql.batch.update.util;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.supplyAsync;

public class AsyncUtil {

    public static <V> List<V> supplyAsyncAll(List<Supplier<V>> suppliers, ExecutorService executorService){
        List<CompletableFuture<V>> futures = suppliers.stream()
                .map(supplier -> supplyAsync(supplier, executorService))
                .collect(Collectors.toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(aVoid -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())).join();
    }

    private static final long WAIT_TIMEOUT_IN_NANOS = Duration.ofSeconds(10).toNanos();

    public static boolean shutdownAndAwaitTermination(ExecutorService service) {
        // Disable new tasks from being submitted
        service.shutdown();
        try {
            long halfTimeoutNanos = WAIT_TIMEOUT_IN_NANOS / 2;
            // Wait for half the duration of the timeout for existing tasks to terminate
            if (!service.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS)) {
                // Cancel currently executing tasks
                service.shutdownNow();
                // Wait the other half of the timeout for tasks to respond to being cancelled
                service.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException ie) {
            // Preserve interrupt status
            Thread.currentThread().interrupt();
            // (Re-)Cancel if current thread also interrupted
            service.shutdownNow();
        }
        return service.isTerminated();
    }

}
