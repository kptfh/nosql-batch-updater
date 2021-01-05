package nosql.batch.update.reactor;

import reactor.core.publisher.Mono;

public interface ReactorUpdateOperations<UPDATES> {

    Mono<Void> updateMany(UPDATES batchOfUpdates, boolean calledByWal);
}
