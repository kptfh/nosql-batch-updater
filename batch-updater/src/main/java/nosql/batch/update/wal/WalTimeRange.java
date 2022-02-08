package nosql.batch.update.wal;

public final class WalTimeRange {
    public final long fromTimestamp;
    public final long toTimestamp;

    public WalTimeRange(long fromTimestamp, long toTimestamp) {
        this.fromTimestamp = fromTimestamp;
        this.toTimestamp = toTimestamp;
    }

    public long getFromTimestamp() {
        return fromTimestamp;
    }

    public long getToTimestamp() {
        return toTimestamp;
    }
}
