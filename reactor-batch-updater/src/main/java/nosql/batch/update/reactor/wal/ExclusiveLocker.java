package nosql.batch.update.reactor.wal;

public interface ExclusiveLocker {

    boolean acquire();
    void release();
    void shutdown();
}
