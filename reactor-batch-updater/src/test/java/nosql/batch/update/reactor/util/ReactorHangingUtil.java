package nosql.batch.update.reactor.util;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CompletableFuture;

import static nosql.batch.update.util.HangingUtil.hanged;

public class ReactorHangingUtil {

    public static <V> Mono<V> hang() {
        return Mono.defer(() -> {
            hanged.set(true);
            return Mono.fromFuture(new CompletableFuture<V>());
        }).publishOn(Schedulers.elastic());
    }
}
