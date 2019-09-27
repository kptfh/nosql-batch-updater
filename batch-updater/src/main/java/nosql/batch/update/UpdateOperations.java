package nosql.batch.update;

public interface UpdateOperations<UPDATES> {

    void updateMany(UPDATES batchOfUpdates);
}
