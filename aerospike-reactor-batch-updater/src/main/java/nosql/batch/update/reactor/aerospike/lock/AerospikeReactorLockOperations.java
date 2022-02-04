package nosql.batch.update.reactor.aerospike.lock;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.aerospike.lock.AerospikeBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.lock.LockingException;
import nosql.batch.update.lock.PermanentLockingException;
import nosql.batch.update.lock.TemporaryLockingException;
import nosql.batch.update.reactor.lock.ReactorLockOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static nosql.batch.update.lock.Lock.LockType.LOCKED;
import static nosql.batch.update.lock.Lock.LockType.SAME_BATCH;

public class AerospikeReactorLockOperations<LOCKS extends AerospikeBatchLocks<EV>, EV>
        implements ReactorLockOperations<LOCKS, AerospikeLock, Value> {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeReactorLockOperations.class);

    private static final String BATCH_ID_BIN_NAME = "batch_id";

    private static final WritePolicy checkValuesPolicy = new WritePolicy();
    static {
        checkValuesPolicy.respondAllOps = true;
    }

    private final IAerospikeReactorClient reactorClient;
    private final WritePolicy putLockPolicy;
    private final WritePolicy deleteLockPolicy;
    private final AerospikeReactorExpectedValuesOperations<EV> expectedValuesOperations;

    public AerospikeReactorLockOperations(IAerospikeReactorClient reactorClient,
                                          AerospikeReactorExpectedValuesOperations<EV> expectedValuesOperations) {
        this.putLockPolicy = configurePutLockPolicy(reactorClient.getWritePolicyDefault());
        this.reactorClient = reactorClient;
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
    public Mono<List<AerospikeLock>> acquire(Value batchId, LOCKS batchLocks, boolean checkBatchId) throws LockingException {
        return putLocks(batchId, batchLocks, checkBatchId)
                .flatMap(keysLocked -> checkExpectedValues(batchLocks, keysLocked)
                        .then(Mono.just(keysLocked)));
    }

    protected Mono<List<AerospikeLock>> putLocks(
            Value batchId,
            LOCKS batchLocks,
            boolean checkTransactionId) {

        return Flux.fromIterable(batchLocks.keysToLock())
                .flatMap(lockKey -> putLock(batchId, lockKey, checkTransactionId)
                        .map(LockResult::new)
                        .onErrorResume(throwable -> Mono.just(new LockResult<>(throwable))))
                .collectList()
                .flatMap(lockResults -> processResults(lockResults));
    }

    static Mono<? extends List<AerospikeLock>> processResults(List<LockResult<AerospikeLock>> lockResults) {
        List<AerospikeLock> locks = new ArrayList<>(lockResults.size());
        Throwable resultError = null;
        for(LockResult<AerospikeLock> lockResult : lockResults){
            if(lockResult.throwable != null){
                if(lockResult.throwable instanceof LockingException){
                    if(resultError == null) {
                        resultError = lockResult.throwable;
                    }
                } else {
                    //give priority to non LockingException
                    resultError = new RuntimeException(lockResult.throwable);
                    break;
                }
            }
            locks.add(lockResult.value);
        }
        if(resultError != null){
            return Mono.error(resultError);
        }
        return Mono.just(locks);
    }

    private Mono<AerospikeLock> putLock(Value batchId, Key lockKey, boolean checkBatchId) {
        return reactorClient.add(putLockPolicy, lockKey, new Bin(BATCH_ID_BIN_NAME, batchId))
                .map(key -> {
                    logger.trace("acquired lock key=[{}], batchId=[{}]", lockKey, batchId);
                    return new AerospikeLock(LOCKED, lockKey);
                })
                .onErrorResume(AerospikeException.class, ae -> {
                    if (ae.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
                        if (checkBatchId) {
                            return alreadyLockedByBatch(lockKey, batchId)
                                    .flatMap(actualBatchId -> {
                                        if(batchId.equals(actualBatchId)){
                                            //check for same batch
                                            //this is used only by WriteAheadLogCompleter to skip already locked keys
                                            logger.info("Previously locked by this batch update key=[{}], batchId=[{}]",
                                                    lockKey, batchId);
                                            return Mono.just(new AerospikeLock(SAME_BATCH, lockKey));
                                        } else {
                                            logger.error("Locked by other batch update but not expected key=[{}], batchId=[{}], actualBatchId=[{}]",
                                                    lockKey, batchId, actualBatchId);
                                            return Mono.error(new TemporaryLockingException(String.format(
                                                    "Locked by this batch update but not expected key=[%s], batchId=[%s], actualBatchId=[%s]",
                                                    lockKey, batchId, actualBatchId)));
                                        }
                                    });
                        } else {
                            return getBatchIdOfLock(lockKey)
                                    .flatMap(batchIdLocked -> {
                                        logger.info("Locked by concurrent update key=[{}], batchId=[{}], batchIdLocked=[{}]",
                                                lockKey, batchId, batchIdLocked);
                                        return Mono.error(new TemporaryLockingException(String.format(
                                                "Locked by concurrent update key=[%s], batchId=[%s], batchIdLocked=[%s]",
                                                lockKey, batchId, batchIdLocked)));
                                    });
                        }
                    } else {
                        logger.error("Unexpected error while acquiring lock key=[{}], batchId=[{}]", lockKey, batchId);
                        return Mono.error(ae);
                    }
                });
    }

    protected Mono<Void> checkExpectedValues(LOCKS batchLocks, List<AerospikeLock> keysLocked) {
        return expectedValuesOperations.checkExpectedValues(keysLocked, batchLocks.expectedValues());
    }

    private Mono<Value> getBatchIdOfLock(Key lockKey){
        return reactorClient.get(null, lockKey)
                .map(keyRecord -> getBatchId(keyRecord.record));
    }

    private Value getBatchId(Record record) {
        return record != null
                ? Value.get(record.getValue(BATCH_ID_BIN_NAME)) :
                //may have place if key get unlocked before we get response
                Value.getAsNull();
    }

    private Mono<Value> alreadyLockedByBatch(Key lockKey, Value batchId) {
        return getBatchIdOfLock(lockKey);
    }

    @Override
    public Mono<List<AerospikeLock>> getLockedByBatchUpdate(LOCKS aerospikeBatchLocks, Value batchId) {
        List<Key> keys = aerospikeBatchLocks.keysToLock();

        Key[] keysArray = keys.toArray(new Key[0]);
        return reactorClient.get(null, keysArray)
                .map(keyRecords -> {
                    List<AerospikeLock> keysFiltered = new ArrayList<>(keys.size());
                    for(int i = 0, m = keysArray.length; i < m; i++){
                        Record record = keyRecords.records[i];
                        if(record != null && batchId.equals(getBatchId(record))){
                            keysFiltered.add(new AerospikeLock(SAME_BATCH, keysArray[i]));
                        }
                    }
                    return keysFiltered;
                });
    }

    @Override
    public Mono<Void> release(Collection<AerospikeLock> locks, Value batchId) {

        return Flux.fromIterable(locks)
                .flatMap(lock -> reactorClient.delete(deleteLockPolicy, lock.key)
                        .doOnNext(key -> logger.trace("released lock key=[{}], batchId=[{}]", key, batchId))
                )
                .then();
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
