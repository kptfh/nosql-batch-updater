package nosql.batch.update;

import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

abstract public class FailingUpdateOperations<UPDATES> implements UpdateOperations<UPDATES>{

    private final UpdateOperations<UPDATES> updateOperations;
    private final AtomicBoolean failsUpdate;

    public FailingUpdateOperations(UpdateOperations<UPDATES> updateOperations, AtomicBoolean failsUpdate) {
        this.updateOperations = updateOperations;
        this.failsUpdate = failsUpdate;
    }

    abstract protected UPDATES selectFlakingToUpdate(UPDATES batchOfUpdates);

    @Override
    public Mono<Void> updateMany(UPDATES batchOfUpdates, boolean calledByWal) {
        if(failsUpdate.get()){
            UPDATES partialUpdate = selectFlakingToUpdate(batchOfUpdates);
            return updateOperations.updateMany(partialUpdate, calledByWal)
                    .then(Mono.error(new RuntimeException()));
        }
        else {
            return updateOperations.updateMany(batchOfUpdates, calledByWal);
        }
    }

}
