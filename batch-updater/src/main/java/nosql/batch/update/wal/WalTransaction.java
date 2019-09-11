package nosql.batch.update.wal;

import nosql.batch.update.BatchUpdate;

abstract class WalTransaction<L, U> implements Comparable<WalTransaction>{

    final TransactionId transactionId;
    final long timestamp;
    final BatchUpdate<L, U> batch;

    public WalTransaction(TransactionId transactionId, long timestamp, BatchUpdate<L, U> batch) {
        this.transactionId = transactionId;
        this.timestamp = timestamp;
        this.batch = batch;
    }

    @Override
    public int compareTo(WalTransaction transaction) {
        return Long.compare(timestamp, transaction.timestamp);
    }

    public BatchUpdate<L, U> getBatchUpdate() {
        return batch;
    }
}
