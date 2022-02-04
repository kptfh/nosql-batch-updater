package nosql.batch.update.aerospike.wal;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import nosql.batch.update.wal.ExclusiveLocker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.temporal.ChronoUnit.SECONDS;
import static nosql.batch.update.aerospike.wal.AerospikeWriteAheadLogManager.getBytesFromUUID;
import static nosql.batch.update.util.AsyncUtil.shutdownAndAwaitTermination;

public class AerospikeExclusiveLocker implements ExclusiveLocker {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeExclusiveLocker.class);

    private static final Instant JAN_01_2010 = Instant.parse("2010-01-01T00:00:00.00Z");

    private static final Value EXCLUSIVE_LOCK_KEY = Value.get((byte)0);

    private final IAerospikeClient client;
    private final Duration exclusiveLockTtl;
    private final ScheduledExecutorService scheduledExecutorService;

    private final WritePolicy putLockPolicy;
    private final Bin exclusiveLockBin;
    private final Key exclusiveLockKey;
    private final AtomicInteger generation = new AtomicInteger(0);
    private final AtomicReference<ScheduledFuture> scheduledFuture = new AtomicReference<>();

    public AerospikeExclusiveLocker(
            IAerospikeClient client, String namespace, String setName) {
        this(client, namespace, setName,
                Executors.newSingleThreadScheduledExecutor(),
                Duration.ofSeconds(60));
    }

    public AerospikeExclusiveLocker(
            IAerospikeClient client, String namespace, String setName,
            ScheduledExecutorService scheduledExecutorService, Duration exclusiveLockTtl) {
        this.client = client;
        this.exclusiveLockTtl = exclusiveLockTtl;
        this.scheduledExecutorService = scheduledExecutorService;

        this.putLockPolicy = buildPutLockPolicy();

        this.exclusiveLockBin = new Bin("EL", getBytesFromUUID(UUID.randomUUID()));

        exclusiveLockKey = new Key(namespace, setName, EXCLUSIVE_LOCK_KEY);
    }

    @Override
    public boolean acquire(){
        if(generation.get() > 0){
            return true;
        }

        try {
            client.put(putLockPolicy, exclusiveLockKey, exclusiveLockBin);
            generation.incrementAndGet();
            logger.info("Successfully got exclusive WAL lock");

            scheduledFuture.set(scheduledExecutorService.scheduleAtFixedRate(this::upgradeLock,
                    exclusiveLockTtl.getSeconds() / 2,
                    exclusiveLockTtl.getSeconds() / 2, TimeUnit.SECONDS));

            return true;
        } catch (AerospikeException e){
            if(e.getResultCode() == ResultCode.KEY_EXISTS_ERROR){
                logger.debug("Failed to get exclusive WAL lock, will try later");
                int expiration = client.get(null, exclusiveLockKey).expiration;
                logger.debug("WAL lock will be released at {}", JAN_01_2010.plus(expiration, SECONDS));
                return false;
            } else {
                logger.error("Failed while getting exclusive WAL lock", e);
                throw e;
            }
        }
    }

    @Override
    public void release() {
        if(generation.get() > 0){
            client.delete(null, exclusiveLockKey);
            reset();
        }
    }

    private WritePolicy buildPutLockPolicy(){
        WritePolicy putLockPolicy = new WritePolicy();
        putLockPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        putLockPolicy.expiration = (int) exclusiveLockTtl.get(SECONDS);
        return putLockPolicy;
    }

    private void upgradeLock(){
        try {
            client.touch(buildTouchLockPolicy(), exclusiveLockKey);
            generation.incrementAndGet();
            logger.info("Successfully upgraded WAL lock");
        } catch (AerospikeException e){
            logger.error("Failed while upgrading WAL lock", e);
            //downgrade lock
            reset();
            throw e;
        }
    }

    private void reset(){
        generation.set(0);

        if(scheduledFuture.get() != null){
            scheduledFuture.get().cancel(false);
            scheduledFuture.set(null);
        }
    }

    private WritePolicy buildTouchLockPolicy(){
        WritePolicy touchLockPolicy = new WritePolicy();
        touchLockPolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        touchLockPolicy.generation = this.generation.get();
        touchLockPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        touchLockPolicy.expiration = (int) exclusiveLockTtl.get(SECONDS);
        return touchLockPolicy;
    }

    @Override
    public void shutdown(){
        shutdownAndAwaitTermination(scheduledExecutorService);
    }

}
