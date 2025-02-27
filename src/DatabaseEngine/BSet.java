package DatabaseEngine;

import java.util.Collections;
import java.util.HashSet;

public class BSet<T extends Comparable<T>> extends HashSet<T>{ //NOTE: avoid comparing by reference
    public BSet(){ super(); }

    public BSet<T> AND(BSet<T> set){
        BSet<T> result = new BSet<>();

        for(T key : this){
            if (set.contains(key))
                result.add(key);
        }

        return result;
    }

    public BSet<T> OR(BSet<T> set){
        BSet<T> result = new BSet<>();

        result.addAll(this);

        for(T key : set){
            if (!this.contains(key))
                result.add(key);
        }

        return result;
    }

    public BSet<T> XOR(BSet<T> set){
        BSet<T> result = new BSet<>();

        for(T key : this){
            if (!set.contains(key))
                result.add(key);
        }

        for(T key : set){
            if (!this.contains(key))
                result.add(key);
        }

        return result;
    }

    public BSet<T> EXCEPT(BSet<T> set){
        BSet<T> result = new BSet<>();

        for(T key : this){
            if (!set.contains(key))
                result.add(key);
        }

        return result;
    }

    public T getMin(){
        if (this.isEmpty()) return null;
        return Collections.min(this);
    }
}
