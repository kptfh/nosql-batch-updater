package nosql.batch.update.aerospike.wal;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.client.async.NioEventLoops;
import nosql.batch.update.BatchUpdate;
import nosql.batch.update.aerospike.lock.AerospikeBatchLocks;
import nosql.batch.update.util.FixedClock;
import nosql.batch.update.wal.WriteAheadLogManagerTest;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeClient;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeContainer;

public class AerospikeWriteAheadLogManagerTest extends WriteAheadLogManagerTest<Value> {

    static final GenericContainer aerospike = getAerospikeContainer();

    static final NioEventLoops eventLoops = new NioEventLoops();
    static final AerospikeClient client = getAerospikeClient(aerospike, eventLoops);

    static final FixedClock clock = new FixedClock();
    static {
        clock.setTime(1000);
    }
    static final Duration staleThreshold = Duration.ofMillis(100);

    static String walSetName = String.valueOf(AerospikeWriteAheadLogManagerTest.class.hashCode());

    private static AerospikeWriteAheadLogManager<AerospikeBatchLocks<Object>, Object, Object> writeAheadLogManager
            =  new AerospikeWriteAheadLogManager<>(
                client, AEROSPIKE_PROPERTIES.getNamespace(), walSetName,
                new AerospikeBatchUpdateSerde<AerospikeBatchLocks<Object>, Object, Object>(){
                    @Override
                    public List<Bin> write(BatchUpdate batch) {
                        return emptyList();
                    }
                    @Override
                    public BatchUpdate read(Map bins) {
                        return null;
                    }
                },
                clock, Executors.newCachedThreadPool());



    @Override
    protected Value saveBatch() {
        return writeAheadLogManager.writeBatch(
                new BatchUpdate<AerospikeBatchLocks<Object>, Object>() {
                    @Override
                    public AerospikeBatchLocks<Object> locks() {
                        return null;
                    }

                    @Override
                    public Object updates() {
                        return null;
                    }
                });
    }

    @Override
    protected boolean removeBatch(Value batchId) {
        return writeAheadLogManager.deleteBatch(batchId);
    }

    @Override
    protected void switchClockAhead() {
        clock.setTime(clock.millis() + staleThreshold.toMillis() + 1);
    }

    @Override
    protected List<Value> getStaleBatches() {
        return writeAheadLogManager.getStaleBatches(staleThreshold).stream()
                .map(record -> record.batchId)
                .collect(Collectors.toList());
    }

}
