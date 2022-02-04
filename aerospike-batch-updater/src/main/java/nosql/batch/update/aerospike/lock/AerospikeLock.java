package nosql.batch.update.aerospike.lock;

import com.aerospike.client.Key;
import nosql.batch.update.lock.Lock;

public class AerospikeLock extends Lock {

    public final Key key;

    public AerospikeLock(LockType lockType, Key key) {
        super(lockType);
        this.key = key;
    }

    @Override
    public boolean equals(Object o){
        AerospikeLock aerospikeLock = (AerospikeLock)o;
        return aerospikeLock.lockType == lockType
                && aerospikeLock.key.equals(key);
    }

    @Override
    public int hashCode(){
        return key.hashCode();
    }
}
