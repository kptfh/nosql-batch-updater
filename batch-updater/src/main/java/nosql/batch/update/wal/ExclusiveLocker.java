package nosql.batch.update.wal;

public interface ExclusiveLocker {

    boolean acquire();
    void release();
    void shutdown();
}
