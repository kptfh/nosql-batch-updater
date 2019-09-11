package nosql.batch.update;

import nosql.batch.update.wal.TransactionId;
import nosql.batch.update.wal.WriteAheadLogManager;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class BatchUpdater<L, U> {

    private final WriteAheadLogManager<L, U> writeAheadLogManager;
    private BatchOperations<L, U> batchOperations;

    public BatchUpdater(BatchOperations<L, U> batchOperations) {
        this.batchOperations = batchOperations;
        this.writeAheadLogManager = batchOperations.getWriteAheadLogManager();
    }

    public void update(BatchUpdate<L, U> batchUpdate) {
        TransactionId transactionId = writeAheadLogManager.writeTransaction(batchUpdate);

        batchOperations.processAndDeleteTransaction(transactionId, batchUpdate, false);
    }

}
