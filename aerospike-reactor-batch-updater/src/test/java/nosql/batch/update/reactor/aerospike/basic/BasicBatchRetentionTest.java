package nosql.batch.update.reactor.aerospike.basic;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.reactor.AerospikeReactorClient;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.BatchRetentionTest;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.reactor.ReactorBatchOperations;
import nosql.batch.update.reactor.ReactorBatchUpdater;
import nosql.batch.update.reactor.wal.ReactorWriteAheadLogCompleter;
import nosql.batch.update.util.FixedClock;
import nosql.batch.update.wal.CompletionStatistic;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.deleteAllRecords;
import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.getAerospikeClient;
import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static nosql.batch.update.reactor.aerospike.basic.BasicConsistencyTest.getValue;
import static nosql.batch.update.reactor.aerospike.basic.BasicConsistencyTest.incrementBoth;
import static nosql.batch.update.reactor.aerospike.basic.util.BasicFailingOperationsUtil.failingOperations;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.ONE_SECOND;

public class BasicBatchRetentionTest extends BatchRetentionTest {

    static final GenericContainer aerospike = getAerospikeContainer();

    static final NioEventLoops eventLoops = new NioEventLoops();
    static final AerospikeClient client = getAerospikeClient(aerospike, eventLoops);
    static final IAerospikeReactorClient reactorClient = new AerospikeReactorClient(client, eventLoops);

    static final FixedClock clock = new FixedClock();

    static ReactorBatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> operations
            = failingOperations(client, reactorClient, clock,
            failsAcquireLock, failsCheckValue, failsMutate, failsReleaseLock, failsDeleteBatch, deletesInProcess);

    static ReactorBatchUpdater<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> updater
            = new ReactorBatchUpdater<>(operations);

    public static final Duration STALE_BATCHES_THRESHOLD = Duration.ofSeconds(1);

    static ReactorWriteAheadLogCompleter<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> walCompleter
            = new ReactorWriteAheadLogCompleter<>(
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
            incrementBoth(key1, key2, updater, client);
        }
    }

    @Override
    protected void checkForConsistency() {
        assertThat(getValue(key1, client)).isEqualTo(getValue(key2, client));

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
