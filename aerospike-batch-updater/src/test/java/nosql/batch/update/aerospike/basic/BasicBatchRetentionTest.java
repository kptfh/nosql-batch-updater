package nosql.batch.update.aerospike.basic;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.reactor.AerospikeReactorClient;
import com.aerospike.client.reactor.IAerospikeReactorClient;
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

import static nosql.batch.update.aerospike.AerospikeTestUtils.*;
import static nosql.batch.update.aerospike.basic.BasicConsistencyTest.getValue;
import static nosql.batch.update.aerospike.basic.BasicConsistencyTest.incrementBoth;
import static nosql.batch.update.aerospike.basic.util.BasicFailingOperationsUtil.failingOperations;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.ONE_SECOND;

public class BasicBatchRetentionTest extends BatchRetentionTest {

    static final GenericContainer aerospike = getAerospikeContainer();

    static final NioEventLoops eventLoops = new NioEventLoops();
    static final AerospikeClient client = getAerospikeClient(aerospike, eventLoops);
    static final IAerospikeReactorClient reactorClient = new AerospikeReactorClient(client, eventLoops);

    static final FixedClock clock = new FixedClock();

    static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> operations
            = failingOperations(client, reactorClient, clock,
            failsAcquireLock, failsCheckValue, failsMutate, failsReleaseLock, failsDeleteBatch, deletesInProcess);

    static BatchUpdater<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> updater
            = new BatchUpdater<>(operations);

    public static final Duration STALE_BATCHES_THRESHOLD = Duration.ofSeconds(1);

    static WriteAheadLogCompleter<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> walCompleter
            = new WriteAheadLogCompleter<>(
            operations, STALE_BATCHES_THRESHOLD,
            new BasicRecoveryTest.DummyExclusiveLocker(),
            Executors.newScheduledThreadPool(1));

    static AtomicInteger keyCounter = new AtomicInteger();

    static String setName = String.valueOf(BasicBatchRetentionTest.class.hashCode());

    private Key key1 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), setName, keyCounter.incrementAndGet());
    private Key key2 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), setName, keyCounter.incrementAndGet());

    @Override
    protected void runUpdate() {
        for(int i = 0; i < 10; i++){
            incrementBoth(key1, key2, updater);
        }
    }

    @Override
    protected void checkForConsistency() {
        assertThat(getValue(client, key1)).isEqualTo(getValue(client, key2));

        await().timeout(ONE_SECOND).untilAsserted(() ->
                assertThat(operations.getWriteAheadLogManager().getStaleBatches(STALE_BATCHES_THRESHOLD)).isEmpty());
    }

    @Override
    protected void cleanUp() throws InterruptedException {
        deleteAllRecords(aerospike);

        clock.setTime(0);
    }

    @Override
    protected CompletionStatistic runCompleter() {
        clock.setTime(STALE_BATCHES_THRESHOLD.toMillis() + 1);
        return walCompleter.completeHangedTransactions();
    }


}
