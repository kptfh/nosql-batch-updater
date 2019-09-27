package nosql.batch.update.aerospike;

import com.aerospike.AerospikeContainerUtils;
import com.aerospike.AerospikeProperties;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import org.testcontainers.containers.GenericContainer;

import java.util.stream.Stream;

public class AerospikeTestUtils {

    public static AerospikeProperties AEROSPIKE_PROPERTIES = new AerospikeProperties();

    public static GenericContainer getAerospikeContainer() {
        return AerospikeContainerUtils.startAerospikeContainer(AEROSPIKE_PROPERTIES);
    }

    static void deleteAllRecords(GenericContainer container) throws InterruptedException {
        try(AerospikeClient client = new AerospikeClient(container.getContainerIpAddress(),
                container.getMappedPort(AEROSPIKE_PROPERTIES.getPort()))) {
            while(!isEmptyNamespace(client, AEROSPIKE_PROPERTIES.getNamespace())){
                truncateNamespace(client, AEROSPIKE_PROPERTIES.getNamespace());
                Thread.sleep(100);
            }
        }
    }

    public static void truncateNamespace(IAerospikeClient client, String namespace) throws InterruptedException {
        while(!isEmptyNamespace(client, namespace)){
            client.truncate(null, namespace, null, null);
            Thread.sleep(100);
        }
    }

    public static boolean isEmptyNamespace(IAerospikeClient client, String namespace){
        String answer = Info.request(client.getNodes()[0], "sets/" + namespace);
        return answer.isEmpty()
                || Stream.of(answer.split(";"))
                .allMatch(s -> s.contains("objects=0"));
    }

}
