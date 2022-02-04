package nosql.batch.update.aerospike.basic;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.NioEventLoops;
import nosql.batch.update.BatchOperations;
import nosql.batch.update.BatchUpdater;
import nosql.batch.update.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.lock.LockingException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.time.Clock;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeClient;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static nosql.batch.update.aerospike.basic.AerospikeBasicBatchUpdater.basicOperations;
import static org.assertj.core.api.Assertions.assertThat;

public class BasicConsistencyTest {

    private static final Logger logger = LoggerFactory.getLogger(BasicConsistencyTest.class);

    private static final GenericContainer aerospike = getAerospikeContainer();

    private static final AerospikeClient client = getAerospikeClient(aerospike, new NioEventLoops());

    private static BatchOperations<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> operations = basicOperations(
            client,
            AEROSPIKE_PROPERTIES.getNamespace(), "wal",
            Clock.systemUTC(),
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool());

    private static BatchUpdater<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> updater = new BatchUpdater<>(operations);

    private static String setName = String.valueOf(BasicConsistencyTest.class.hashCode());
    private static AtomicInteger keyCounter = new AtomicInteger();
    private static String BIN_NAME = "value";

    private AtomicInteger exceptionsCount = new AtomicInteger();
    private Random random = new Random();
    private Key key1 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), setName, keyCounter.incrementAndGet());
    private Key key2 = new Key(AEROSPIKE_PROPERTIES.getNamespace(), setName, keyCounter.incrementAndGet());

    @Test
    public void shouldUpdate() {
        update(key1, key2);

        assertThat((Long)client.get(null, key1).getValue(BIN_NAME)).isEqualTo(1000);
        assertThat((Long)client.get(null, key2).getValue(BIN_NAME)).isEqualTo(1000);
    }

    @Test
    public void shouldUpdateConcurrently() throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future future1 = executorService.submit(() -> update(key1, key2));
        Future future2 = executorService.submit(() -> update(key1, key2));

        future1.get();
        future2.get();

        assertThat((Long)client.get(null, key1).getValue(BIN_NAME)).isEqualTo(2000);
        assertThat((Long)client.get(null, key2).getValue(BIN_NAME)).isEqualTo(2000);
        assertThat(exceptionsCount.get()).isGreaterThan(0);
    }

    private void update(Key key1, Key key2){
        for(int i = 0; i < 1000; i++){
            try {
                incrementBoth(key1, key2, updater, client);
            } catch (LockingException e) {
                exceptionsCount.incrementAndGet();
                i--;
                try {
                    Thread.sleep(random.nextInt(25));
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }

                logger.debug(e.getMessage());
            }
        }
    }

    public static void incrementBoth(Key key1, Key key2,
                                     BatchUpdater<AerospikeBasicBatchLocks, List<Record>, AerospikeLock, Value> updater,
                                     AerospikeClient aerospikeClient) {
        Long value1 = (Long)getValue(key1, aerospikeClient);
        Long value2 = (Long)getValue(key2, aerospikeClient);

        long value1New = (value1 != null ? value1 : 0) + 1;
        long value2New = (value2 != null ? value2 : 0) + 1;
        updater.update(new AerospikeBasicBatchUpdate(
                new AerospikeBasicBatchLocks(asList(
                        record(key1, value1),
                        record(key2, value2))),
                asList(
                        record(key1, value1New),
                        record(key2, value2New))));
        logger.debug("updated {} from {} to {} and {} from {} to {}", key1, value1, value1New, key2, value2, value2New);
    }

    public static Record record(Key key, Long value) {
        return new Record(key, singletonList(new Bin(BIN_NAME, value)));
    }

    public static Object getValue(Key key, AerospikeClient client){
        com.aerospike.client.Record record1 = client.get(null, key);
        return record1 != null ? (Long)record1.getValue(BIN_NAME) : null;
    }
}
