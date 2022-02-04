package nosql.batch.update.util;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

public class FixedClock extends Clock {

    private final AtomicLong time = new AtomicLong();

    @Override
    public ZoneId getZone() {
        return null;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return null;
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(time.get());
    }

    public void setTime(long time) {
        this.time.set(time);
    }
}

