package nosql.batch.update;

import reactor.core.publisher.Mono;

public interface UpdateOperations<UPDATES> {

    Mono<Void> updateMany(UPDATES batchOfUpdates);
}
