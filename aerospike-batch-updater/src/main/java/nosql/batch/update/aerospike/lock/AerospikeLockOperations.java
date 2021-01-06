package nosql.batch.update.aerospike.lock;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import nosql.batch.update.lock.LockOperations;
import nosql.batch.update.lock.LockingException;
import nosql.batch.update.lock.PermanentLockingException;
import nosql.batch.update.lock.TemporaryLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static nosql.batch.update.lock.Lock.LockType.LOCKED;
import static nosql.batch.update.lock.Lock.LockType.SAME_BATCH;

public class AerospikeLockOperations<LOCKS extends AerospikeBatchLocks<EV>, EV> implements LockOperations<LOCKS, AerospikeLock, Value> {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeLockOperations.class);

    private static final String BATCH_ID_BIN_NAME = "batch_id";

    private static final WritePolicy checkValuesPolicy = new WritePolicy();
    static {
        checkValuesPolicy.respondAllOps = true;
    }

    private final IAerospikeClient aerospikeClient;
    private final WritePolicy putLockPolicy;
    private final WritePolicy deleteLockPolicy;
    private final AerospikeExpectedValuesOperations<EV> expectedValuesOperations;
    private final ExecutorService aerospikeExecutor;

    public AerospikeLockOperations(IAerospikeClient aerospikeClient,
                                   AerospikeExpectedValuesOperations<EV> expectedValuesOperations,
                                   ExecutorService aerospikeExecutor) {
        this.putLockPolicy = configurePutLockPolicy(aerospikeClient.getWritePolicyDefault());
        this.aerospikeClient = aerospikeClient;
        this.aerospikeExecutor = aerospikeExecutor;
        this.deleteLockPolicy = putLockPolicy;
        this.expectedValuesOperations = expectedValuesOperations;
    }

    private WritePolicy configurePutLockPolicy(WritePolicy writePolicyDefault){
        WritePolicy writePolicy = new WritePolicy(writePolicyDefault);
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        writePolicy.expiration = -1;
        return writePolicy;
    }

    @Override
    public List<AerospikeLock> acquire(Value batchId, LOCKS batchLocks, boolean checkBatchId) throws LockingException {
        List<AerospikeLock> keysLocked = putLocks(batchId, batchLocks, checkBatchId);
        checkExpectedValues(batchLocks, keysLocked);
        return keysLocked;
    }

    protected List<AerospikeLock> putLocks(
            Value batchId,
            LOCKS batchLocks,
            boolean checkTransactionId) throws LockingException {

        List<Key> keys = batchLocks.keysToLock();
        List<CompletableFuture<LockResult<AerospikeLock>>> futures = new ArrayList<>(keys.size());
        AtomicReference<Throwable> fail = new AtomicReference<>();
        for(Key lockKey : keys){
            futures.add(supplyAsync(() -> {
                try {
                    if(fail.get() != null){
                        return null;
                    }
                    AerospikeLock lock = putLock(batchId, lockKey, checkTransactionId);
                    return new LockResult<>(lock);
                } catch (Throwable t) {
                    fail.set(t);
                    return new LockResult<>(t);
                }
            }, aerospikeExecutor));
        }

        allOf(futures.toArray(new CompletableFuture<?>[0])).join();

        return processResults(futures);
    }

    static List<AerospikeLock> processResults(
            List<CompletableFuture<LockResult<AerospikeLock>>> lockResults) {
        List<AerospikeLock> locks = new ArrayList<>(lockResults.size());
        LockingException resultError = null;
        for(CompletableFuture<LockResult<AerospikeLock>> future : lockResults){
            LockResult<AerospikeLock> lockResult = future.join();
            if(lockResult != null) {
                if (lockResult.throwable != null) {
                    if (lockResult.throwable instanceof LockingException) {
                        if (resultError == null) {
                            resultError = (LockingException) lockResult.throwable;
                        }
                    } else {
                        //give priority to non LockingException
                        resultError = new PermanentLockingException(lockResult.throwable);
                        break;
                    }
                }
                locks.add(lockResult.value);
            }
        }
        if(resultError != null){
            throw resultError;
        }
        return locks;
    }

    private AerospikeLock putLock(Value batchId, Key lockKey, boolean checkBatchId) {
        try {
            aerospikeClient.add(putLockPolicy, lockKey, new Bin(BATCH_ID_BIN_NAME, batchId));
            logger.trace("acquired lock key=[{}], batchId=[{}]", lockKey, batchId);
            return new AerospikeLock(LOCKED, lockKey);
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
                if (checkBatchId) {
                    if(alreadyLockedByBatch(lockKey, batchId)){
                        //check for same batch
                        //this is used only by WriteAheadLogCompleter to skip already locked keys
                        logger.info("Previously locked by this batch update key=[{}], batchId=[{}]",
                                lockKey, batchId);
                        return new AerospikeLock(SAME_BATCH, lockKey);
                    } else {
                        logger.error("Locked by this batch update but not expected key=[{}], batchId=[{}]",
                                lockKey, batchId);
                        throw new TemporaryLockingException(String.format(
                                "Locked by this batch update but not expected key=[%s], batchId=[%s]",
                                lockKey, batchId));
                    }
                } else {
                    Value batchIdLocked = getBatchIdOfLock(lockKey);
                    logger.info("Locked by concurrent update key=[{}], batchId=[{}], batchIdLocked=[{}]",
                            lockKey, batchId, batchIdLocked);
                    throw new TemporaryLockingException(String.format(
                            "Locked by concurrent update key=[%s], batchId=[%s], batchIdLocked=[%s]",
                            lockKey, batchId, batchIdLocked));
                }
            } else {
                logger.error("Unexpected error while acquiring lock key=[{}], batchId=[{}]", lockKey, batchId);
                throw ae;
            }
        }
    }

    protected void checkExpectedValues(LOCKS batchLocks, List<AerospikeLock> keysLocked) {
        expectedValuesOperations.checkExpectedValues(keysLocked, batchLocks.expectedValues());
    }

    private Value getBatchIdOfLock(Key lockKey){
        Record record = aerospikeClient.get(null, lockKey);
        return getBatchId(record);
    }

    private Value getBatchId(Record record) {
        return record != null
                ? Value.get(record.getValue(BATCH_ID_BIN_NAME)) :
                //may have place if key get unlocked before we get response
                Value.getAsNull();
    }

    private Boolean alreadyLockedByBatch(Key lockKey, Value batchId) {
        return batchId.equals(getBatchIdOfLock(lockKey));
    }

    @Override
    public List<AerospikeLock> getLockedByBatchUpdate(LOCKS aerospikeBatchLocks, Value batchId) {
        List<Key> keys = aerospikeBatchLocks.keysToLock();

        Key[] keysArray = keys.toArray(new Key[0]);
        //using executor to not use all connections to aerospike node
        Record[] records = supplyAsync(() -> aerospikeClient.get(null, keysArray), aerospikeExecutor).join();

        List<AerospikeLock> keysFiltered = new ArrayList<>(keys.size());
        for(int i = 0, m = keysArray.length; i < m; i++){
            Record record = records[i];
            if(record != null && batchId.equals(getBatchId(record))){
                keysFiltered.add(new AerospikeLock(SAME_BATCH, keysArray[i]));
            }
        }
        return keysFiltered;
    }

    @Override
    public void release(List<AerospikeLock> locks, Value batchId) {
        List<CompletableFuture<Void>> futures = new ArrayList<>(locks.size());
        for(AerospikeLock lock : locks){
            futures.add(runAsync(() -> releaseLock(lock, batchId), aerospikeExecutor));
        }
        allOf(futures.toArray(new CompletableFuture<?>[0])).join();
    }

    protected void releaseLock(AerospikeLock lock, Value batchId) {
        aerospikeClient.delete(deleteLockPolicy, lock.key);
        logger.trace("released lock key=[{}], batchId=[{}]", lock.key, batchId);
    }

    public static class LockResult<V> {
        public final V value;
        public final Throwable throwable;

        public LockResult(V value) {
            this.value = value;
            this.throwable = null;
        }

        public LockResult(Throwable throwable) {
            this.value = null;
            this.throwable = throwable;
        }
    }


}
