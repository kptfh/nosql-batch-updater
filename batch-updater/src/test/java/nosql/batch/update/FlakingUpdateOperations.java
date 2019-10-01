package nosql.batch.update;

import java.util.concurrent.atomic.AtomicBoolean;

abstract public class FlakingUpdateOperations<UPDATES> implements UpdateOperations<UPDATES>{

    private final UpdateOperations<UPDATES> updateOperations;
    private final AtomicBoolean failsUpdate;

    public FlakingUpdateOperations(UpdateOperations<UPDATES> updateOperations, AtomicBoolean failsUpdate) {
        this.updateOperations = updateOperations;
        this.failsUpdate = failsUpdate;
    }

    abstract protected UPDATES selectFlakingToUpdate(UPDATES batchOfUpdates);

    @Override
    public void updateMany(UPDATES batchOfUpdates) {
        if(failsUpdate.get()){
            UPDATES partialUpdate = selectFlakingToUpdate(batchOfUpdates);
            updateOperations.updateMany(partialUpdate);

            throw new RuntimeException();
        } else {
            updateOperations.updateMany(batchOfUpdates);
        }
    }

}
