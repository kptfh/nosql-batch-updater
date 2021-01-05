package nosql.batch.update.reactor.aerospike.basic;

import nosql.batch.update.ReactorFailingUpdateOperations;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.reactor.ReactorUpdateOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.util.HangingUtil.selectFlaking;

public class AerospikeBasicFailingUpdateOperations extends ReactorFailingUpdateOperations<List<Record>> {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeBasicFailingUpdateOperations.class);

    public AerospikeBasicFailingUpdateOperations(ReactorUpdateOperations<List<Record>> updateOperations, AtomicBoolean failsUpdate) {
        super(updateOperations, failsUpdate);
    }

    public static AerospikeBasicFailingUpdateOperations failingUpdates(
            ReactorUpdateOperations<List<Record>> updateOperations, AtomicBoolean failsUpdate){
        return new AerospikeBasicFailingUpdateOperations(updateOperations, failsUpdate);
    }

    @Override
    protected List<Record> selectFlakingToUpdate(List<Record> records) {
        return selectFlaking(records,
                key -> logger.info("batch update failed flaking for key [{}]", key));
    }
}
