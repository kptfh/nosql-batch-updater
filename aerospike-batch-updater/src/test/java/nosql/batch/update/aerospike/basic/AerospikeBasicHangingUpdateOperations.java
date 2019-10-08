package nosql.batch.update.aerospike.basic;

import nosql.batch.update.HangingUpdateOperations;
import nosql.batch.update.UpdateOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.util.HangingUtil.selectFlaking;

public class AerospikeBasicHangingUpdateOperations extends HangingUpdateOperations<List<Record>> {

    private static Logger logger = LoggerFactory.getLogger(AerospikeBasicHangingUpdateOperations.class);

    public AerospikeBasicHangingUpdateOperations(UpdateOperations<List<Record>> updateOperations, AtomicBoolean failsUpdate) {
        super(updateOperations, failsUpdate);
    }

    public static AerospikeBasicHangingUpdateOperations hangingUpdates(
            UpdateOperations<List<Record>> updateOperations, AtomicBoolean failsUpdate){
        return new AerospikeBasicHangingUpdateOperations(updateOperations, failsUpdate);
    }

    @Override
    protected List<Record> selectFlakingToUpdate(List<Record> records) {
        return selectFlaking(records,
                key -> logger.info("batch update failed flaking for key [{}]", key));
    }
}
