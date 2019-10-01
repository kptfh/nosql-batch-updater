package nosql.batch.update.aerospike.basic;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

import java.util.List;

public class Record {

    public final Key key;
    public final List<Bin> bins;

    public Record(Key key, List<Bin> bins) {
        this.key = key;
        this.bins = bins;
    }

}
