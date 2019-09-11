package nosql.batch.update;

public interface UpdateOperations<U> {

    void updateMany(U batchOfUpdates);
}
