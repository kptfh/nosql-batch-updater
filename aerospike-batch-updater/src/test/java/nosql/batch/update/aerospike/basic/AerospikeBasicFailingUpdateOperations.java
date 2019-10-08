package nosql.batch.update.aerospike.basic;

import nosql.batch.update.FailingUpdateOperations;
import nosql.batch.update.UpdateOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.util.HangingUtil.selectFlaking;

public class AerospikeBasicFailingUpdateOperations extends FailingUpdateOperations<List<Record>> {

    private static Logger logger = LoggerFactory.getLogger(AerospikeBasicFailingUpdateOperations.class);

    public AerospikeBasicFailingUpdateOperations(UpdateOperations<List<Record>> updateOperations, AtomicBoolean failsUpdate) {
        super(updateOperations, failsUpdate);
    }

    public static AerospikeBasicFailingUpdateOperations failingUpdates(
            UpdateOperations<List<Record>> updateOperations, AtomicBoolean failsUpdate){
        return new AerospikeBasicFailingUpdateOperations(updateOperations, failsUpdate);
    }

    @Override
    protected List<Record> selectFlakingToUpdate(List<Record> records) {
        return selectFlaking(records,
                key -> logger.info("batch update failed flaking for key [{}]", key));
    }
}
