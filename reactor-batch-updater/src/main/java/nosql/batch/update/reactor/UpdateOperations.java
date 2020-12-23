package nosql.batch.update.reactor;

import reactor.core.publisher.Mono;

public interface UpdateOperations<UPDATES> {

    Mono<Void> updateMany(UPDATES batchOfUpdates, boolean calledByWal);
}
