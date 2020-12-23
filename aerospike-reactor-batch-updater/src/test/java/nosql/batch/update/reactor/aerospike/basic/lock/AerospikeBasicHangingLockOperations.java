package nosql.batch.update.reactor.aerospike.basic.lock;

import com.aerospike.client.Value;
import nosql.batch.update.reactor.aerospike.basic.Record;
import nosql.batch.update.reactor.aerospike.lock.AerospikeLock;
import nosql.batch.update.reactor.lock.HangingLockOperations;
import nosql.batch.update.reactor.lock.LockOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.reactor.util.HangingUtil.selectFlaking;

public class AerospikeBasicHangingLockOperations
        extends HangingLockOperations<AerospikeBasicBatchLocks, AerospikeLock, Value> {

    private static Logger logger = LoggerFactory.getLogger(AerospikeBasicHangingLockOperations.class);

    private AerospikeBasicHangingLockOperations(LockOperations<AerospikeBasicBatchLocks, AerospikeLock, Value> lockOperations,
                                                AtomicBoolean failsAcquire, AtomicBoolean failsRelease) {
        super(lockOperations, failsAcquire, failsRelease);
    }

    public static AerospikeBasicHangingLockOperations hangingLocks(LockOperations<AerospikeBasicBatchLocks, AerospikeLock, Value> lockOperations,
                                                                   AtomicBoolean failsAcquire, AtomicBoolean failsRelease){
        return new AerospikeBasicHangingLockOperations(lockOperations, failsAcquire, failsRelease);
    }

    @Override
    protected AerospikeBasicBatchLocks selectFlakingToAcquire(AerospikeBasicBatchLocks aerospikeBasicBatchLocks) {
        List<Record> recordsSelected = selectFlaking(aerospikeBasicBatchLocks.expectedValues(),
                key -> logger.info("acquire locks failed flaking for key [{}]", key));

        return new AerospikeBasicBatchLocks(recordsSelected);
    }

    @Override
    protected Collection<AerospikeLock> selectFlakingToRelease(Collection<AerospikeLock> locks) {
        return selectFlaking(locks,
                key -> logger.info("release locks failed flaking for key [{}]", key));
    }


}
