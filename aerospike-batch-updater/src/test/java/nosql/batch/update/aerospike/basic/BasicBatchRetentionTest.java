package nosql.batch.update.aerospike.basic;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.NioEventLoops;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.BatchRetentionTest;
import nosql.batch.update.BatchUpdater;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.util.FixedClock;
import nosql.batch.update.wal.CompletionStatistic;
import nosql.batch.update.wal.WriteAheadLogCompleter;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeClient;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static nosql.batch.update.aerospike.basic.BasicConsistencyTest.getValue;
import static nosql.batch.update.aerospike.basic.BasicConsistencyTest.incrementBoth;
import static nosql.batch.update.aerospike.basic.util.BasicFailingOperationsUtil.failingOperations;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.ONE_SECOND;

public class BasicBatchRetentionTest extends BatchRetentionTest {

    static final GenericContainer aerospike = getAerospikeContainer();

    static final AerospikeClient client = getAerospikeClient(aerospike, new NioEventLoops());

    static final FixedClock clock = new FixedClock();

    static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> operations
            = failingOperations(client, clock, Executors.newCachedThreadPool(),
            failsAcquireLock, failsCheckValue, failsMutate, failsReleaseLock, failsDeleteBatch, deletesInProcess);

    static BatchUpdater<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> updater
            = new BatchUpdater<>(operations);

    public static final Duration STALE_BATCHES_THRESHOLD = Duration.ofSeconds(1);
    public static final int BATCH_SIZE = 100;

    static WriteAheadLogCompleter<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> walCompleter
            = new WriteAheadLogCompleter<>(
            operations, STALE_BATCHES_THRESHOLD, BATCH_SIZE,
            new BasicRecoveryTest.DummyExclusiveLocker(),
            Executors.newScheduledThreadPool(1));

    static AtomicInteger keyCounter = new AtomicInteger();

    private Key key1;
    private Key key2;

    @Override
    protected void runUpdate() {
        for(int i = 0; i < 10; i++){
            incrementBoth(key1, key2, updater, client);
        }
    }

    @Override
    protected void checkForConsistency() {
        assertThat(getValue(key1, client)).isEqualTo(getValue(key2, client));

        await().timeout(ONE_SECOND).untilAsserted(() ->
                assertThat(operations.getWriteAheadLogManager().getTimeRanges(STALE_BATCHES_THRESHOLD, BATCH_SIZE)).isEmpty());
    }

    private int setNameCounter = 0;

    @Override
    protected void cleanUp() {
        String setName = String.valueOf(setNameCounter++);
        key1 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), setName, keyCounter.incrementAndGet());
        key2 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), setName, keyCounter.incrementAndGet());

        clock.setTime(0);
    }

    @Override
    protected CompletionStatistic runCompleter() {
        clock.setTime(STALE_BATCHES_THRESHOLD.toMillis() + 1);
        return walCompleter.completeHangedTransactions();
    }


}
