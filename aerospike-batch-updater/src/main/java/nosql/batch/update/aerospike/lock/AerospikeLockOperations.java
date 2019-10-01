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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static nosql.batch.update.lock.Lock.LockType.LOCKED;
import static nosql.batch.update.lock.Lock.LockType.SAME_BATCH;
import static nosql.batch.update.util.AsyncUtil.supplyAsyncAll;

public class AerospikeLockOperations<LOCKS extends AerospikeBatchLocks<EV>, EV> implements LockOperations<LOCKS, AerospikeLock, Value> {

    private static Logger logger = LoggerFactory.getLogger(AerospikeLockOperations.class);

    private static final String BATCH_ID_BIN_NAME = "batch_id";

    private static final WritePolicy checkValuesPolicy = new WritePolicy();
    static {
        checkValuesPolicy.respondAllOps = true;
    }

    private final IAerospikeClient client;
    private final IAerospikeReactorClient reactorClient;
    private final WritePolicy putLockPolicy;
    private final WritePolicy deleteLockPolicy;
    private final AerospikeExpectedValuesOperations<EV> expectedValuesOperations;
    private final ExecutorService executorService;

    public AerospikeLockOperations(IAerospikeClient client,
                                   IAerospikeReactorClient reactorClient,
                                   AerospikeExpectedValuesOperations<EV> expectedValuesOperations, ExecutorService executorService) {
        this.client = client;
        this.putLockPolicy = configurePutLockPolicy(client.getWritePolicyDefault());
        this.reactorClient = reactorClient;
        this.deleteLockPolicy = putLockPolicy;
        this.expectedValuesOperations = expectedValuesOperations;
        this.executorService = executorService;
    }

    private WritePolicy configurePutLockPolicy(WritePolicy writePolicyDefault){
        WritePolicy writePolicy = new WritePolicy(writePolicyDefault);
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        writePolicy.expiration = -1;
        return writePolicy;
    }

    @Override
    public List<AerospikeLock> acquire(Value batchId, LOCKS batchLocks, boolean checkBatchId,
                                       Consumer<Collection<AerospikeLock>> onErrorCleaner) throws LockingException {
        List<AerospikeLock> keysLocked = putLocks(batchId, batchLocks, checkBatchId, onErrorCleaner);
        try {
            expectedValuesOperations.checkExpectedValues(keysLocked, batchLocks.expectedValues());
        } catch (Throwable t){
            onErrorCleaner.accept(keysLocked);
            throw t;
        }
        return keysLocked;
    }

    private List<AerospikeLock> putLocks(
            Value batchId,
            AerospikeBatchLocks<EV> batchLocks,
            boolean checkTransactionId,
            Consumer<Collection<AerospikeLock>> onErrorCleanup) throws LockingException {

        List<ExecResult<AerospikeLock>> lockResults = supplyAsyncAll(
                batchLocks.keysToLock().stream()
                        .map(lockKey -> (Supplier<ExecResult<AerospikeLock>>)() -> {
                            try {
                                return new ExecResult<>(putLock(batchId, lockKey, checkTransactionId));
                            } catch (Throwable t) {
                                return new ExecResult<>(t);
                            }
                        })
                        .collect(Collectors.toList()), executorService);

        List<AerospikeLock> locks = new ArrayList<>();
        Throwable throwable = null;
        for(ExecResult<AerospikeLock> result : lockResults){
            if(result.throwable != null){
                throwable = result.throwable;
            } else {
                locks.add(result.value);
            }
        }

        if(throwable != null){
            onErrorCleanup.accept(locks);
            if(throwable instanceof LockingException){
                throw (LockingException)throwable;
            } else {
                throw new PermanentLockingException(throwable);
            }
        }

        return locks;
    }

    private AerospikeLock putLock(Value batchId, Key lockKey, boolean checkBatchId) {
        try {
            client.add(putLockPolicy, lockKey, new Bin(BATCH_ID_BIN_NAME, batchId));
            logger.trace("acquired lock key=[{}], batchId=[{}]", lockKey, batchId);
            return new AerospikeLock(LOCKED, lockKey);
        } catch (AerospikeException e) {
            if (e.getResultCode() == ResultCode.KEY_EXISTS_ERROR){
                if(checkBatchId && alreadyLockedByBatch(lockKey, batchId)){
                    //check for same batch
                    //this is used only by WriteAheadLogCompleter to skip already locked keys
                    logger.info("Previously locked by this batch update key=[{}], batchId=[{}]", lockKey, batchId);
                    return new AerospikeLock(SAME_BATCH, lockKey);
                } else {
                    Value batchIdLocked = getBatchIdOfLock(lockKey);
                    logger.info("Locked by concurrent update key=[{}], batchId=[{}]", lockKey, batchIdLocked);
                    throw new TemporaryLockingException(String.format(
                            "Locked by concurrent update key=[%s], batchId=[%s]", lockKey, batchIdLocked));
                }
            }
            throw e;
        }
    }

    private Value getBatchIdOfLock(Key lockKey){
        Record record = client.get(null, lockKey);
        return getBatchId(record);
    }

    private Value getBatchId(Record record) {
        return Value.get(record.getValue(BATCH_ID_BIN_NAME));
    }

    private boolean alreadyLockedByBatch(Key lockKey, Value batchId) {
        return batchId.equals(getBatchIdOfLock(lockKey));
    }

    @Override
    public List<AerospikeLock> getLockedByBatchUpdate(LOCKS aerospikeBatchLocks, Value batchId) {
        List<Key> keys = aerospikeBatchLocks.keysToLock();

        List<AerospikeLock> keysFiltered = new ArrayList<>(keys.size());
        Key[] keysArray = keys.toArray(new Key[0]);
        Record[] records = client.get(null, keysArray);
        for(int i = 0, m = keysArray.length; i < m; i++){
            Record record = records[i];
            if(record != null && batchId.equals(getBatchId(record))){
                keysFiltered.add(new AerospikeLock(SAME_BATCH, keysArray[i]));
            }
        }
        return keysFiltered;
    }

    @Override
    public Mono<Void> release(Collection<AerospikeLock> locks, Value batchId) {

        List<CompletableFuture<?>> futures = new ArrayList<>(locks.size());
        for(AerospikeLock lock : locks){
            futures.add(runAsync(() -> {
                client.delete(deleteLockPolicy, lock.key);

            }, executorService));
        }
        allOf(futures.toArray(new CompletableFuture[0])).join();

        return Flux.fromIterable(locks)
                .flatMap(lock -> reactorClient.delete(deleteLockPolicy, lock.key)
                        .doOnNext(key -> logger.trace("released lock key=[{}], batchId=[{}]", key, batchId))
                )
                .collect(Collectors.counting())
                .then();
    }

    public static class ExecResult<V> {
        public final V value;
        public final Throwable throwable;

        public ExecResult(V value) {
            this.value = value;
            this.throwable = null;
        }

        public ExecResult(Throwable throwable) {
            this.value = null;
            this.throwable = throwable;
        }
    }

}
