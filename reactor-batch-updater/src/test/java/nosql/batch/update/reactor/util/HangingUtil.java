package nosql.batch.update.reactor.util;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class HangingUtil {

    private static final Random random = new Random();
    public static final AtomicBoolean hanged = new AtomicBoolean();

    public static <V> List<V> selectFlaking(Collection<V> keys, Consumer<V> failedConsumer) {
        return keys.stream()
                .filter(key -> {
                    boolean fail = random.nextBoolean();
                    if (fail) {
                        failedConsumer.accept(key);
                    }
                    return !fail;
                })
                .collect(Collectors.toList());
    }

    public static <V> Mono<V> hang() {
        return Mono.defer(() -> {
            hanged.set(true);
            return Mono.fromFuture(new CompletableFuture<V>());
        }).publishOn(Schedulers.elastic());
    }
}
