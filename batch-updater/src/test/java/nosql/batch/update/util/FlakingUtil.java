package nosql.batch.update.util;

import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class FlakingUtil {

    private static final Random random = new Random();

    public static <V> List<V> selectFlaking(Collection<V> keys, Consumer<V> failedConsumer) {
        return keys.stream()
                .filter(key -> {
                    boolean fail = random.nextBoolean();
                    if (fail) {
                        failedConsumer.accept(key);
                    }
                    return !fail;
                })
                .collect(Collectors.toList());
    }
}
