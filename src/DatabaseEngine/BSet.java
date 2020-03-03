package DatabaseEngine;

import java.util.HashSet;

public class BSet<T> extends HashSet<T>{
    public BSet(){ super(); }

    public BSet<T> AND(BSet<T> set){
        BSet<T> result = new BSet<>();

        for(T key : this){
            if (set.contains(key))
                result.add(key);
        }

        return result;z
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



}
