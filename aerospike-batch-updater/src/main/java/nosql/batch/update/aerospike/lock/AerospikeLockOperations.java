package nosql.batch.update.aerospike.lock;

import com.aerospike.client.*;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.lock.LockOperations;
import nosql.batch.update.lock.LockingException;
import nosql.batch.update.lock.PermanentLockingException;
import nosql.batch.update.lock.TemporaryLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static nosql.batch.update.lock.Lock.LockType.LOCKED;
import static nosql.batch.update.lock.Lock.LockType.SAME_BATCH;

public class AerospikeLockOperations<LOCKS extends AerospikeBatchLocks<EV>, EV> implements LockOperations<LOCKS, AerospikeLock, Value> {

    private static Logger logger = LoggerFactory.getLogger(AerospikeLockOperations.class);

    private static final String BATCH_ID_BIN_NAME = "batch_id";

    private static final WritePolicy checkValuesPolicy = new WritePolicy();
    static {
        checkValuesPolicy.respondAllOps = true;
    }

    private final IAerospikeReactorClient reactorClient;
    private final WritePolicy putLockPolicy;
    private final WritePolicy deleteLockPolicy;
    private final AerospikeExpectedValuesOperations<EV> expectedValuesOperations;

    public AerospikeLockOperations(IAerospikeReactorClient reactorClient,
                                   AerospikeExpectedValuesOperations<EV> expectedValuesOperations) {
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
    public Mono<List<AerospikeLock>> acquire(Value batchId, LOCKS batchLocks, boolean checkBatchId,
                                             Function<LOCKS, Mono<Void>> onErrorCleaner) throws LockingException {
        return putLocks(batchId, batchLocks, checkBatchId, onErrorCleaner)
                .flatMap(keysLocked -> checkExpectedValues(batchLocks, keysLocked)
                        .onErrorResume(throwable -> onErrorCleaner.apply(batchLocks)
                                .then(Mono.error(throwable)))
                        .then(Mono.just(keysLocked)));
    }

    protected Mono<List<AerospikeLock>> putLocks(
            Value batchId,
            LOCKS batchLocks,
            boolean checkTransactionId,
            Function<LOCKS, Mono<Void>> onErrorCleanup) throws LockingException {

        return Flux.fromIterable(batchLocks.keysToLock())
                .flatMap(lockKey -> putLock(batchId, lockKey, checkTransactionId))
                .onErrorResume(throwable ->
                        onErrorCleanup.apply(batchLocks)
                                .then(Mono.error(() -> {
                                    if(throwable instanceof LockingException){
                                        return throwable;
                                    } else {
                                        return new PermanentLockingException(throwable);
                                    }
                                })))
                .collectList();
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
                                    .<AerospikeLock>flatMap(lockedByThisBatch -> {
                                        if(lockedByThisBatch){
                                            //check for same batch
                                            //this is used only by WriteAheadLogCompleter to skip already locked keys
                                            logger.info("Previously locked by this batch update key=[{}], batchId=[{}]",
                                                    lockKey, batchId);
                                            return Mono.just(new AerospikeLock(SAME_BATCH, lockKey));
                                        } else {
                                            logger.error("Locked by this batch update but not expected key=[{}], batchId=[{}]",
                                                    lockKey, batchId);
                                            return Mono.error(new TemporaryLockingException(String.format(
                                                    "Locked by this batch update but not expected key=[%s], batchId=[%s]",
                                                    lockKey, batchId)));
                                        }
                                    });

                        } else {
                            return getBatchIdOfLock(lockKey)
                                    .flatMap(batchIdLocked -> {
                                        logger.info("Locked by concurrent update key=[{}], batchId=[{}]", lockKey, batchIdLocked);
                                        return Mono.error(new TemporaryLockingException(String.format(
                                                "Locked by concurrent update key=[%s], batchId=[%s]", lockKey, batchIdLocked)));
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
        return Value.get(record.getValue(BATCH_ID_BIN_NAME));
    }

    private Mono<Boolean> alreadyLockedByBatch(Key lockKey, Value batchId) {
        return getBatchIdOfLock(lockKey).map(batchId::equals);
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

}
