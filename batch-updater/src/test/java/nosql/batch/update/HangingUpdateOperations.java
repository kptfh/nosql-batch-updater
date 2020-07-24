package nosql.batch.update;

import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.util.HangingUtil.hang;

abstract public class HangingUpdateOperations<UPDATES> implements UpdateOperations<UPDATES>{

    private final UpdateOperations<UPDATES> updateOperations;
    private final AtomicBoolean hangUpdate;

    public HangingUpdateOperations(UpdateOperations<UPDATES> updateOperations, AtomicBoolean hangUpdate) {
        this.updateOperations = updateOperations;
        this.hangUpdate = hangUpdate;
    }

    abstract protected UPDATES selectFlakingToUpdate(UPDATES batchOfUpdates);

    @Override
    public Mono<Void> updateMany(UPDATES batchOfUpdates, boolean calledByWal) {
        if(hangUpdate.get()){
            UPDATES partialUpdate = selectFlakingToUpdate(batchOfUpdates);
            return updateOperations.updateMany(partialUpdate, calledByWal)
                    .then(hang());
        }
        else {
            return updateOperations.updateMany(batchOfUpdates, calledByWal);
        }
    }

}
