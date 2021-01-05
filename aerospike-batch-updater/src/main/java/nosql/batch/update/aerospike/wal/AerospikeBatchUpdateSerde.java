package nosql.batch.update.aerospike.wal;

import com.aerospike.client.Bin;
import nosql.batch.update.BatchUpdate;
import nosql.batch.update.aerospike.lock.AerospikeBatchLocks;

import java.util.List;
import java.util.Map;

public interface AerospikeBatchUpdateSerde<LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV> {

    List<Bin> write(BatchUpdate<LOCKS, UPDATES> batch);

    BatchUpdate<LOCKS, UPDATES> read(Map<String, Object> bins);
}
