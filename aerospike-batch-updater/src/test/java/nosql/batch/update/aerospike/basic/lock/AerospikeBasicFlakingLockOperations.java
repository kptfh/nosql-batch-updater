package nosql.batch.update.aerospike.basic.lock;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.aerospike.lock.AerospikeLockOperations;
import nosql.batch.update.lock.FlakingLockOperations;
import nosql.batch.update.lock.LockOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.util.FlakingUtil.selectFlaking;

public class AerospikeBasicFlakingLockOperations
        extends FlakingLockOperations<AerospikeBasicBatchLocks, AerospikeLock, Value> {

    private static Logger logger = LoggerFactory.getLogger(AerospikeBasicFlakingLockOperations.class);

    private AerospikeBasicFlakingLockOperations(LockOperations<AerospikeBasicBatchLocks, AerospikeLock, Value> lockOperations,
                                               AtomicBoolean failsAcquire, AtomicBoolean failsRelease) {
        super(lockOperations, failsAcquire, failsRelease);
    }

    public static AerospikeBasicFlakingLockOperations flakingLocks(LockOperations<AerospikeBasicBatchLocks, AerospikeLock, Value> lockOperations,
                                                                   AtomicBoolean failsAcquire, AtomicBoolean failsRelease){
        return new AerospikeBasicFlakingLockOperations(lockOperations, failsAcquire, failsRelease);
    }

    @Override
    protected AerospikeBasicBatchLocks selectFlakingToAcquire(AerospikeBasicBatchLocks aerospikeBasicBatchLocks) {
        return new AerospikeBasicBatchLocks(aerospikeBasicBatchLocks.expectedValues()){
            @Override
            public List<Key> keysToLock() {
                return selectFlaking(super.keysToLock(),
                        key -> logger.info("acquire locks failed flaking for key [{}]", key));
            }
        };
    }

    @Override
    protected Collection<AerospikeLock> selectFlakingToRelease(Collection<AerospikeLock> locks) {
        return selectFlaking(locks,
                key -> logger.info("release locks failed flaking for key [{}]", key));
    }


}
