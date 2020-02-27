package nosql.batch.update.aerospike.basic;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.reactor.AerospikeReactorClient;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.BatchUpdater;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.lock.LockingException;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static nosql.batch.update.aerospike.AerospikeTestUtils.*;
import static nosql.batch.update.aerospike.basic.AerospikeBasicBatchUpdater.basicOperations;
import static org.assertj.core.api.Assertions.assertThat;

public class BasicConsistencyTest {

    static final GenericContainer aerospike = getAerospikeContainer();

    static final NioEventLoops eventLoops = new NioEventLoops();
    static final AerospikeClient client = getAerospikeClient(aerospike, eventLoops);
    static final IAerospikeReactorClient reactorClient = new AerospikeReactorClient(client, eventLoops);

    static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> operations = basicOperations(
            client, reactorClient,
            AEROSPIKE_PROPERTIES.getNamespace(), "wal",
            Clock.systemUTC());

    static BatchUpdater<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> updater = new BatchUpdater<>(operations);

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
            try {
                incrementBoth(key1, key2, updater);
            } catch (LockingException e) {
                exceptionsCount.incrementAndGet();
                i--;
                try {
                    Thread.sleep(random.nextInt(50));
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    public static void incrementBoth(Key key1, Key key2,
                                     BatchUpdater<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> updater) {
        Long value1 = (Long)getValue(client, key1);
        Long value2 = (Long)getValue(client, key2);

        updater.update(new AerospikeBasicBatchUpdate(
                new AerospikeBasicBatchLocks(asList(
                        record(key1, value1),
                        record(key2, value2))),
                asList(
                        record(key1, (value1 != null ? value1 : 0) + 1),
                        record(key2, (value2 != null ? value2 : 0) + 1))))
                .block();
    }

    public static Record record(Key key, Long value) {
        return new Record(key, singletonList(new Bin(BIN_NAME, value)));
    }

    public static Object getValue(AerospikeClient client, Key key){
        com.aerospike.client.Record record1 = client.get(null, key);
        return record1 != null ? (Long)record1.getValue(BIN_NAME) : null;
    }
}
