package nosql.batch.update.aerospike.wal;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import nosql.batch.update.BatchUpdate;
import nosql.batch.update.aerospike.lock.AerospikeBatchLocks;
import nosql.batch.update.wal.WalRecord;
import nosql.batch.update.wal.WriteAheadLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

public class AerospikeWriteAheadLogManager<LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
        implements WriteAheadLogManager<LOCKS, UPDATES, Value> {

    private static final Logger logger = LoggerFactory.getLogger(AerospikeWriteAheadLogManager.class);

    private static final String UUID_BIN_NAME = "uuid";
    private static final String TIMESTAMP_BIN_NAME = "timestamp";

    private final IAerospikeClient client;
    private final String walNamespace;
    private final String walSetName;
    private final WritePolicy writePolicy;
    private final WritePolicy deletePolicy;
    private final AerospikeBatchUpdateSerde<LOCKS, UPDATES, EV> batchSerializer;
    private final Clock clock;

    public AerospikeWriteAheadLogManager(IAerospikeClient client,
                                         String walNamespace, String walSetName,
                                         AerospikeBatchUpdateSerde<LOCKS, UPDATES, EV> batchSerializer,
                                         Clock clock) {
        this.client = client;
        this.walNamespace = walNamespace;
        this.walSetName = walSetName;
        this.writePolicy = configureWritePolicy(client.getWritePolicyDefault());
        this.deletePolicy = this.writePolicy;
        this.batchSerializer = batchSerializer;
        this.clock = clock;

        createSecondaryIndexOnTimestamp();
    }

    private WritePolicy configureWritePolicy(WritePolicy writePolicyDefault){
        WritePolicy writePolicy = new WritePolicy(writePolicyDefault);
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        writePolicy.sendKey = true;
        writePolicy.expiration = -1;
        return writePolicy;
    }

    @Override
    public Value writeBatch(BatchUpdate<LOCKS, UPDATES> batch) {
        Value batchId = generateBatchId();

        List<Bin> batchBins = batchSerializer.write(batch);
        List<Bin> bins = new ArrayList<>(batchBins.size() + 1);
        bins.addAll(batchBins);
        bins.add(new Bin(UUID_BIN_NAME, batchId));
        bins.add(new Bin(TIMESTAMP_BIN_NAME, Value.get(clock.millis())));

        try {
            client.put(writePolicy,
                        new Key(walNamespace, walSetName, batchId),
                        bins.toArray(new Bin[0]));
            return batchId;
        } catch (AerospikeException ae){
            if(ae.getResultCode() == ResultCode.RECORD_TOO_BIG){
                logger.error("update data size to big: {}", batchBins.stream().mapToInt(bin -> bin.value.estimateSize()).sum());
            }
            throw ae;
        }
    }

    public static Value generateBatchId() {
        return Value.get(getBytesFromUUID(UUID.randomUUID()));
    }

    @Override
    public boolean deleteBatch(Value batchId) {
        return client.delete(deletePolicy, new Key(walNamespace, walSetName, batchId));
    }

    @Override
    public List<WalRecord<LOCKS, UPDATES, Value>> getStaleBatches(Duration staleThreshold) {
        Statement statement = staleBatchesStatement(staleThreshold, walNamespace, walSetName, clock);
        RecordSet recordSet = client.query(null, statement);

        List<WalRecord<LOCKS, UPDATES, Value>> staleTransactions = new ArrayList<>();
        recordSet.iterator().forEachRemaining(keyRecord -> {
            Record record = keyRecord.record;
            staleTransactions.add(new WalRecord<>(
                    Value.get(record.getValue(UUID_BIN_NAME)),
                    record.getLong(TIMESTAMP_BIN_NAME),
                    batchSerializer.read(record.bins)));
        });
        Collections.sort(staleTransactions);

        return staleTransactions;
    }

    public static Statement staleBatchesStatement(Duration staleThreshold, String walNamespace, String walSetName, Clock clock) {
        Statement statement = new Statement();
        statement.setNamespace(walNamespace);
        statement.setSetName(walSetName);
        statement.setFilter(Filter.range(TIMESTAMP_BIN_NAME,
                0,  Math.max(clock.millis() - staleThreshold.toMillis(), 0)));
        return statement;
    }

    static byte[] getBytesFromUUID(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());

        return bb.array();
    }

    private void createSecondaryIndexOnTimestamp() {
        try {
            String indexName = walSetName + "_timestamp";
            client.createIndex(null, walNamespace, walSetName, indexName, TIMESTAMP_BIN_NAME, IndexType.NUMERIC)
                    .waitTillComplete(200, 0);
        } catch (AerospikeException ae) {
            if(ae.getResultCode() == ResultCode.INDEX_ALREADY_EXISTS){
                logger.info("Will not create WAL secondary index as it already exists");
            } else {
                throw ae;
            }
        }
    }

    public String getWalNamespace() {
        return walNamespace;
    }

    public String getWalSetName() {
        return walSetName;
    }

    public IAerospikeClient getClient() {
        return client;
    }
}
