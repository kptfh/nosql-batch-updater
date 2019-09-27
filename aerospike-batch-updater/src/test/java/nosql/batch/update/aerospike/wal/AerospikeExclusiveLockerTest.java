package nosql.batch.update.aerospike.wal;

import com.aerospike.client.AerospikeClient;
import nosql.batch.update.wal.ExclusiveLocker;
import nosql.batch.update.wal.ExclusiveLockerTest;
import org.testcontainers.containers.GenericContainer;

import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeContainer;

public class AerospikeExclusiveLockerTest extends ExclusiveLockerTest {

    static final GenericContainer aerospike = getAerospikeContainer();

    static final AerospikeClient client = new AerospikeClient(aerospike.getContainerIpAddress(),
            aerospike.getMappedPort(AEROSPIKE_PROPERTIES.getPort()));

    @Override
    public ExclusiveLocker getExclusiveLocker(){
        return new AerospikeExclusiveLocker(client, AEROSPIKE_PROPERTIES.getNamespace(), "test");
    }
}
