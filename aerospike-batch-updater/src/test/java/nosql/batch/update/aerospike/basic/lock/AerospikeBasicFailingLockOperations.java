package nosql.batch.update.aerospike.basic.lock;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.lock.AerospikeExpectedValuesOperations;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.aerospike.lock.AerospikeLockOperations;
import nosql.batch.update.lock.LockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static nosql.batch.update.util.HangingUtil.selectFlaking;


public class AerospikeBasicFailingLockOperations
        extends AerospikeLockOperations<AerospikeBasicBatchLocks, List<Record>> {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeBasicFailingLockOperations.class);

    private final AtomicBoolean failsAcquire;
    private final AtomicBoolean failsCheckValue;
    private final AtomicBoolean failsRelease;

    public AerospikeBasicFailingLockOperations(IAerospikeClient reactorClient,
                                               ExecutorService aerospikeExecutor,
                                               AerospikeExpectedValuesOperations<List<Record>> expectedValuesOperations,
                                               AtomicBoolean failsAcquire,
                                               AtomicBoolean failsCheckValue,
                                               AtomicBoolean failsRelease) {
        super(reactorClient, expectedValuesOperations, aerospikeExecutor);
        this.failsAcquire = failsAcquire;
        this.failsCheckValue = failsCheckValue;
        this.failsRelease = failsRelease;
    }

    @Override
    protected List<AerospikeLock> putLocks(
            Value batchId,
            AerospikeBasicBatchLocks batchLocks,
            boolean checkTransactionId) throws LockingException {
        if(failsAcquire.get()){
            List<Record> recordsSelected = selectFlaking(batchLocks.expectedValues(),
                    key -> logger.info("acquire locks failed flaking for key [{}]", key));

            super.putLocks(batchId,
                    new AerospikeBasicBatchLocks(recordsSelected),
                    checkTransactionId);
            throw new RuntimeException();
        } else {
            return super.putLocks(batchId, batchLocks, checkTransactionId);
        }
    }

    @Override
    protected void checkExpectedValues(AerospikeBasicBatchLocks batchLocks, List<AerospikeLock> keysLocked) {
        if(failsCheckValue.get()){
            throw new RuntimeException();
        } else {
            super.checkExpectedValues(batchLocks, keysLocked);
        }
    }

    @Override
    public void release(List<AerospikeLock> locks, Value batchId) {
        if(failsRelease.get()){
            List<AerospikeLock> partialLocks = selectFlaking(locks,
                    key -> logger.info("release locks failed flaking for key [{}]", key));
            super.release(partialLocks, batchId);
            throw new RuntimeException();
        } else {
            super.release(locks, batchId);
        }
    }

}
