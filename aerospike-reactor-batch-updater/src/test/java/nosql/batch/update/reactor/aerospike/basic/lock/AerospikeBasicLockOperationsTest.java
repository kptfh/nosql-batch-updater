package nosql.batch.update.reactor.aerospike.basic.lock;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.reactor.AerospikeReactorClient;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.reactor.aerospike.wal.AerospikeReactorWriteAheadLogManager;
import nosql.batch.update.reactor.lock.ReactorLockOperationsTest;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.GenericContainer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.getAerospikeClient;
import static nosql.batch.update.reactor.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static nosql.batch.update.reactor.aerospike.basic.AerospikeBasicBatchUpdater.basicLockOperations;
import static nosql.batch.update.reactor.aerospike.basic.BasicConsistencyTest.record;
import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeBasicLockOperationsTest
        extends ReactorLockOperationsTest<AerospikeBasicBatchLocks, AerospikeLock, Value> {

    static final GenericContainer aerospike = getAerospikeContainer();

    static final NioEventLoops eventLoops = new NioEventLoops();
    static final AerospikeClient client = getAerospikeClient(aerospike, eventLoops);
    static final IAerospikeReactorClient reactorClient = new AerospikeReactorClient(client, eventLoops);

    static String setName = String.valueOf(AerospikeBasicLockOperationsTest.class.hashCode());
    static AtomicInteger keyCounter = new AtomicInteger();
    private Key key1 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), setName, keyCounter.incrementAndGet());
    private Key key2 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), setName, keyCounter.incrementAndGet());
    private Key key3 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), setName, keyCounter.incrementAndGet());
    private Key key4 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), setName, keyCounter.incrementAndGet());


    AerospikeBasicBatchLocks locks1 = new AerospikeBasicBatchLocks(asList(record(key1, null), record(key2, null)));

    public AerospikeBasicLockOperationsTest() {
        super(basicLockOperations(reactorClient));
    }

    @Override
    protected AerospikeBasicBatchLocks getLocks1() {
        return locks1;
    }

    @Override
    protected Value generateBatchId() {
        return AerospikeReactorWriteAheadLogManager.generateBatchId();
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
