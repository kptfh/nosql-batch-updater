package nosql.batch.update.aerospike.lock;

import com.aerospike.client.Key;

import java.util.List;

public interface AerospikeBatchLocks<EV> {

    List<Key> keysToLock();
    EV expectedValues();

}
