package nosql.batch.update.aerospike.basic;

import nosql.batch.update.FlakingUpdateOperations;
import nosql.batch.update.UpdateOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.util.FlakingUtil.selectFlaking;

public class AerospikeBasicFlakingUpdateOperations extends FlakingUpdateOperations<List<Record>> {

    private static Logger logger = LoggerFactory.getLogger(AerospikeBasicFlakingUpdateOperations.class);

    public AerospikeBasicFlakingUpdateOperations(UpdateOperations<List<Record>> updateOperations, AtomicBoolean failsUpdate) {
        super(updateOperations, failsUpdate);
    }

    public static AerospikeBasicFlakingUpdateOperations flakingUpdates(
            UpdateOperations<List<Record>> updateOperations, AtomicBoolean failsUpdate){
        return new AerospikeBasicFlakingUpdateOperations(updateOperations, failsUpdate);
    }

    @Override
    protected List<Record> selectFlakingToUpdate(List<Record> records) {
        return selectFlaking(records,
                key -> logger.info("batch update failed flaking for key [{}]", key));
    }
}
