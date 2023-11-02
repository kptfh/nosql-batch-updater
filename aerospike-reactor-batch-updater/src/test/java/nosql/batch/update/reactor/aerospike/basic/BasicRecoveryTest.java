package nosql.batch.update.reactor.aerospike.basic;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.reactor.AerospikeReactorClient;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.RecoveryTest;
import nosql.batch.update.aerospike.AerospikeTestUtils;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.reactor.ReactorBatchOperations;
import nosql.batch.update.reactor.ReactorBatchUpdater;
import nosql.batch.update.reactor.wal.ReactorWriteAheadLogCompleter;
import nosql.batch.update.util.FixedClock;
import nosql.batch.update.wal.CompletionStatistic;
import nosql.batch.update.wal.ExclusiveLocker;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;

import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.getAerospikeClient;
import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static nosql.batch.update.reactor.aerospike.basic.BasicConsistencyTest.getValue;
import static nosql.batch.update.reactor.aerospike.basic.BasicConsistencyTest.incrementBoth;
import static nosql.batch.update.reactor.aerospike.basic.util.BasicHangingOperationsUtil.hangingOperations;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.ONE_SECOND;

public class BasicRecoveryTest extends RecoveryTest {

    static final GenericContainer aerospike = getAerospikeContainer();

    static final NioEventLoops eventLoops = new NioEventLoops();
    static final AerospikeClient client = getAerospikeClient(aerospike, eventLoops);
    static final IAerospikeReactorClient reactorClient = new AerospikeReactorClient(client, eventLoops);

    static final FixedClock clock = new FixedClock();

    static ReactorBatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> operations
            = hangingOperations(client, reactorClient, clock, hangsAcquire, hangsUpdate, hangsRelease, hangsDeleteBatchInWal);

    static ReactorBatchUpdater<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> updater
            = new ReactorBatchUpdater<>(operations);

    public static final Duration STALE_BATCHES_THRESHOLD = Duration.ofSeconds(1);
    public static final int BATCH_SIZE = 100;

    static ReactorWriteAheadLogCompleter<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> walCompleter
            = new ReactorWriteAheadLogCompleter<>(
            operations, STALE_BATCHES_THRESHOLD, BATCH_SIZE,
            new DummyExclusiveLocker(),
            Executors.newScheduledThreadPool(1));

    private Key key1;
    private Key key2;

    @Override
    protected void runUpdate() {
        for(int i = 0; i < 10; i++){
            incrementBoth(key1, key2, updater, client);
        }
    }

    @Override
    protected CompletionStatistic runCompleter(){
        clock.setTime(STALE_BATCHES_THRESHOLD.toMillis() + 1);
        return walCompleter.completeHangedTransactions();
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
        key1 = new Key(AerospikeTestUtils.AEROSPIKE_PROPERTIES.getNamespace(), setName, 0);
        key2 = new Key(AerospikeTestUtils.AEROSPIKE_PROPERTIES.getNamespace(), setName, 1);

        clock.setTime(0);
    }

    static class DummyExclusiveLocker implements ExclusiveLocker{

        @Override
        public boolean acquire() {
            return true;
        }

        @Override
        public void release() {}

        @Override
        public void shutdown() {}
    }
}
