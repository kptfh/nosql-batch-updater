package nosql.batch.update.wal;


import nosql.batch.update.BatchUpdate;

import java.util.List;

public interface WriteAheadLogManager<L, U> {

    TransactionId writeTransaction(BatchUpdate<L, U> batch);

    void deleteTransaction(TransactionId transactionId);

    List<WalTransaction<L, U>> getStaleTransactions();

}
