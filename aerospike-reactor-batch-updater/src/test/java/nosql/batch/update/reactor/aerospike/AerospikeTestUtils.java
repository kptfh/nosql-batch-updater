package nosql.batch.update.reactor.aerospike;

import com.aerospike.AerospikeContainerUtils;
import com.aerospike.AerospikeProperties;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.policy.ClientPolicy;
import org.testcontainers.containers.GenericContainer;

public class AerospikeTestUtils {

    public static AerospikeProperties AEROSPIKE_PROPERTIES = new AerospikeProperties();

    public static GenericContainer getAerospikeContainer() {
        return AerospikeContainerUtils.startAerospikeContainer(AEROSPIKE_PROPERTIES);
    }

    public static AerospikeClient getAerospikeClient(GenericContainer aerospike) {
        return new AerospikeClient(aerospike.getContainerIpAddress(),
                aerospike.getMappedPort(AEROSPIKE_PROPERTIES.getPort()));
    }

    public static AerospikeClient getAerospikeClient(GenericContainer aerospike, EventLoops eventLoops) {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.eventLoops = eventLoops;
        return new AerospikeClient(clientPolicy, aerospike.getContainerIpAddress(),
                aerospike.getMappedPort(AEROSPIKE_PROPERTIES.getPort()));
    }
}
