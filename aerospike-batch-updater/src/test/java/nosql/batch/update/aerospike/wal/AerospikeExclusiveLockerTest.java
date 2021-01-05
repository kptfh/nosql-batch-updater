package nosql.batch.update.aerospike.wal;

import com.aerospike.client.AerospikeClient;
import nosql.batch.update.wal.ExclusiveLocker;
import nosql.batch.update.wal.ExclusiveLockerTest;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.concurrent.Executors;

import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeExclusiveLockerTest extends ExclusiveLockerTest {

    static final GenericContainer aerospike = getAerospikeContainer();

    static final AerospikeClient client = new AerospikeClient(aerospike.getContainerIpAddress(),
            aerospike.getMappedPort(AEROSPIKE_PROPERTIES.getPort()));

    @Override
    public ExclusiveLocker getExclusiveLocker(){
        return new AerospikeExclusiveLocker(client, AEROSPIKE_PROPERTIES.getNamespace(), "test");
    }

    @Test
    public void shouldUpgradeLock() throws InterruptedException {
        ExclusiveLocker exclusiveLocker = getExclusiveLocker(Duration.ofSeconds(2));

        ExclusiveLocker exclusiveLocker2 = getExclusiveLocker(Duration.ofSeconds(2));

        assertThat(exclusiveLocker.acquire()).isTrue();

        Thread.sleep(2500);

        assertThat(exclusiveLocker2.acquire()).isFalse();
        assertThat(exclusiveLocker.acquire()).isTrue();

        exclusiveLocker.release();
        exclusiveLocker2.release();

        exclusiveLocker.shutdown();
        exclusiveLocker2.shutdown();
    }

    public ExclusiveLocker getExclusiveLocker(Duration exclusiveLockTtl){
        return new AerospikeExclusiveLocker(client, AEROSPIKE_PROPERTIES.getNamespace(), "test",
                Executors.newSingleThreadScheduledExecutor(), exclusiveLockTtl);
    }
}
