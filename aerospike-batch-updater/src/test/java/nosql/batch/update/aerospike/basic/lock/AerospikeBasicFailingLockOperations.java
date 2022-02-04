package nosql.batch.update.aerospike.basic.lock;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.lock.AerospikeExpectedValuesOperations;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.aerospike.lock.AerospikeLockOperations;
import nosql.batch.update.lock.PermanentLockingException;
import nosql.batch.update.lock.TemporaryLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static nosql.batch.update.util.HangingUtil.selectFlaking;


public class AerospikeBasicFailingLockOperations
        extends AerospikeLockOperations<AerospikeBasicBatchLocks, List<Record>> {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeBasicFailingLockOperations.class);

    private final AtomicReference<Throwable> failsAcquire;
    private final AtomicReference<Throwable> failsCheckValue;
    private final AtomicBoolean failsRelease;

    public AerospikeBasicFailingLockOperations(IAerospikeClient reactorClient,
                                               ExecutorService aerospikeExecutor,
                                               AerospikeExpectedValuesOperations<List<Record>> expectedValuesOperations,
                                               AtomicReference<Throwable> failsAcquire,
                                               AtomicReference<Throwable> failsCheckValue,
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
            boolean checkTransactionId) throws TemporaryLockingException {
        Throwable throwable = failsAcquire.get();
        if(throwable != null){
            List<Record> recordsSelected = selectFlaking(batchLocks.expectedValues(),
                    key -> logger.info("acquire locks failed flaking for key [{}]", key));

            super.putLocks(batchId,
                    new AerospikeBasicBatchLocks(recordsSelected),
                    checkTransactionId);
            throw throwable instanceof TemporaryLockingException
                    ? (TemporaryLockingException) throwable
                    : new RuntimeException(throwable);
        } else {
            return super.putLocks(batchId, batchLocks, checkTransactionId);
        }
    }

    @Override
    protected void checkExpectedValues(AerospikeBasicBatchLocks batchLocks, List<AerospikeLock> keysLocked) throws PermanentLockingException {
        Throwable throwable = failsCheckValue.get();
        if(throwable != null){
            throw throwable instanceof PermanentLockingException
                    ? (PermanentLockingException) throwable
                    : new RuntimeException(throwable);
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
