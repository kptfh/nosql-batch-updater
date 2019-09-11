package nosql.batch.update.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class AsyncUtil {

    public static boolean shutdownAndAwaitTermination(
            ExecutorService service, long timeout, TimeUnit unit) {
        // Disable new tasks from being submitted
        service.shutdown();
        try {
            long halfTimeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit) / 2;
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
