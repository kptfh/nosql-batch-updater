package nosql.batch.update.wal;

public class CompletionStatistic {

    public final int staleBatchesFound;
    public final int staleBatchesComplete;
    public final int staleBatchesIgnored;
    public final int staleBatchesErrors;

    public CompletionStatistic(int staleBatchesFound, int staleBatchesComplete, int staleBatchesIgnored, int staleBatchesErrors) {
        this.staleBatchesFound = staleBatchesFound;
        this.staleBatchesComplete = staleBatchesComplete;
        this.staleBatchesIgnored = staleBatchesIgnored;
        this.staleBatchesErrors = staleBatchesErrors;
    }

}
