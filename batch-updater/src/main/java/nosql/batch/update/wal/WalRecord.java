package nosql.batch.update.wal;

import nosql.batch.update.BatchUpdate;

public final class WalRecord<LOCKS, UPDATES, BATCH_ID> implements Comparable<WalRecord>{

    public final BATCH_ID batchId;
    public final long timestamp;
    public final BatchUpdate<LOCKS, UPDATES> batchUpdate;

    public WalRecord(BATCH_ID batchId, long timestamp, BatchUpdate<LOCKS, UPDATES> batchUpdate) {
        this.batchId = batchId;
        this.timestamp = timestamp;
        this.batchUpdate = batchUpdate;
    }

    @Override
    public int compareTo(WalRecord transaction) {
        return Long.compare(timestamp, transaction.timestamp);
    }

}
