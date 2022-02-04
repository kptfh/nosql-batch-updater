package nosql.batch.update;

import nosql.batch.update.reactor.ReactorUpdateOperations;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.reactor.util.ReactorHangingUtil.hang;

abstract public class ReactorHangingUpdateOperations<UPDATES> implements ReactorUpdateOperations<UPDATES> {

    private final ReactorUpdateOperations<UPDATES> updateOperations;
    private final AtomicBoolean hangUpdate;

    public ReactorHangingUpdateOperations(ReactorUpdateOperations<UPDATES> updateOperations, AtomicBoolean hangUpdate) {
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
