package nosql.batch.update.aerospike.simple;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.BatchUpdater;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.aerospike.simple.lock.AerospikeSimpleBatchLocks;
import nosql.batch.update.lock.LockingException;
import nosql.batch.update.lock.TemporaryLockingException;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.time.Clock;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static nosql.batch.update.aerospike.simple.AerospikeSimpleBatchUpdaters.postLockOperations;
import static org.assertj.core.api.Assertions.assertThat;

public class ConcurrentSimpleTest {

    static final GenericContainer aerospike = getAerospikeContainer();

    static final AerospikeClient client = new AerospikeClient(aerospike.getContainerIpAddress(),
            aerospike.getMappedPort(AEROSPIKE_PROPERTIES.getPort()));

    static BatchOperations<AerospikeSimpleBatchLocks, List<Record>, AerospikeLock, Value> operations = postLockOperations(
            client,
            AEROSPIKE_PROPERTIES.getNamespace(), "wal",
            Clock.systemUTC(),
            Executors.newFixedThreadPool(4));

    static BatchUpdater<AerospikeSimpleBatchLocks, List<Record>, AerospikeLock, Value> updater = new BatchUpdater<>(operations);

    static AtomicInteger keyCounter = new AtomicInteger();
    private Key key1 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), "testset", keyCounter.incrementAndGet());
    private Key key2 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), "testset", keyCounter.incrementAndGet());
    static String BIN_NAME = "value";

    private AtomicInteger exceptionsCount = new AtomicInteger();
    private Random random = new Random();

    @Test
    public void shouldUpdate() {
        update();

        assertThat((Long)client.get(null, key1).getValue(BIN_NAME)).isEqualTo(1000);
        assertThat((Long)client.get(null, key2).getValue(BIN_NAME)).isEqualTo(1000);
    }

    @Test
    public void shouldUpdateConcurrently() throws ExecutionException, InterruptedException {
        Future future1 = Executors.newFixedThreadPool(2).submit(this::update);
        Future future2 = Executors.newFixedThreadPool(2).submit(this::update);

        future1.get();
        future2.get();

        assertThat((Long)client.get(null, key1).getValue(BIN_NAME)).isEqualTo(2000);
        assertThat((Long)client.get(null, key2).getValue(BIN_NAME)).isEqualTo(2000);
        assertThat(exceptionsCount.get()).isGreaterThan(0);
    }

    private void update(){
        for(int i = 0; i < 1000; i++){
            com.aerospike.client.Record record1 = client.get(null, key1);
            Long value1 = record1 != null ? (Long)record1.getValue(BIN_NAME) : null;
            com.aerospike.client.Record record2 = client.get(null, key2);
            Long value2 = record2 != null ? (Long)record2.getValue(BIN_NAME) : null;

            try {
                updater.update(new AerospikeSimpleBatchUpdate(
                        new AerospikeSimpleBatchLocks(asList(
                                new Record(key1, singletonList(new Bin(BIN_NAME, value1))),
                                new Record(key2, singletonList(new Bin(BIN_NAME, value2))))),
                        asList(
                                new Record(key1, singletonList(new Bin(BIN_NAME, (value1 != null ? value1 : 0) + 1))),
                                new Record(key2, singletonList(new Bin(BIN_NAME, (value2 != null ? value2 : 0) + 1)))))
                );
            } catch (LockingException e) {
                exceptionsCount.incrementAndGet();
                i--;
                try {
                    Thread.sleep(random.nextInt(100));
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }
}
