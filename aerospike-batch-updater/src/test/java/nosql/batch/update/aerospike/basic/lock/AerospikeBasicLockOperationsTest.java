package nosql.batch.update.aerospike.basic.lock;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.NioEventLoops;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.aerospike.wal.AerospikeWriteAheadLogManager;
import nosql.batch.update.lock.LockOperationsTest;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.GenericContainer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeClient;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static nosql.batch.update.aerospike.basic.AerospikeBasicBatchUpdater.basicLockOperations;
import static nosql.batch.update.aerospike.basic.BasicConsistencyTest.record;
import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeBasicLockOperationsTest
        extends LockOperationsTest<AerospikeBasicBatchLocks, AerospikeLock, Value> {

    static final GenericContainer aerospike = getAerospikeContainer();

    static final NioEventLoops eventLoops = new NioEventLoops();
    static final AerospikeClient client = getAerospikeClient(aerospike, eventLoops);

    static String setName = String.valueOf(AerospikeBasicLockOperationsTest.class.hashCode());
    static AtomicInteger keyCounter = new AtomicInteger();
    private Key key1 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), setName, keyCounter.incrementAndGet());
    private Key key2 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), setName, keyCounter.incrementAndGet());

    AerospikeBasicBatchLocks locks1 = new AerospikeBasicBatchLocks(asList(record(key1, null), record(key2, null)));

    public AerospikeBasicLockOperationsTest() {
        super(basicLockOperations(client, Executors.newCachedThreadPool()));
    }

    @Override
    protected AerospikeBasicBatchLocks getLocks1() {
        return locks1;
    }

    @Override
    protected Value generateBatchId() {
        return AerospikeWriteAheadLogManager.generateBatchId();
    }

    @Override
    protected void assertThatSameLockKeys(List<AerospikeLock> locks1, List<AerospikeLock> locks2) {
        assertThat(toKeys(locks1)).containsExactlyInAnyOrderElementsOf(toKeys(locks2));
    }

    @NotNull
    private Set<Key> toKeys(List<AerospikeLock> locks1) {
        return locks1.stream().map(l -> l.key).collect(Collectors.toSet());
    }
}
