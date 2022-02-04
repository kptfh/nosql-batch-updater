package nosql.batch.update.reactor.aerospike.basic;

import nosql.batch.update.ReactorHangingUpdateOperations;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.reactor.ReactorUpdateOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.util.HangingUtil.selectFlaking;

public class AerospikeBasicHangingUpdateOperations extends ReactorHangingUpdateOperations<List<Record>> {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeBasicHangingUpdateOperations.class);

    public AerospikeBasicHangingUpdateOperations(ReactorUpdateOperations<List<Record>> updateOperations, AtomicBoolean failsUpdate) {
        super(updateOperations, failsUpdate);
    }

    public static AerospikeBasicHangingUpdateOperations hangingUpdates(
            ReactorUpdateOperations<List<Record>> updateOperations, AtomicBoolean failsUpdate){
        return new AerospikeBasicHangingUpdateOperations(updateOperations, failsUpdate);
    }

    @Override
    protected List<Record> selectFlakingToUpdate(List<Record> records) {
        return selectFlaking(records,
                key -> logger.info("batch update failed flaking for key [{}]", key));
    }
}
