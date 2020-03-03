package DatabaseEngine;

import java.util.HashSet;

public class Set<T> extends HashSet<T>{
    public Set(){ super(); }

    public Set<T> AND(Set<T> set){
        Set<T> result = new Set<>();

        for(T key : this){
            if (set.contains(key))
                result.add(key);
        }

        return result;
    }

    public Set<T> OR(Set<T> set){
        Set<T> result = new Set<>();

        result.addAll(this);

        for(T key : set){
            if (!this.contains(key))
                result.add(key);
        }

        return result;
    }

    public Set<T> XOR(Set<T> set){
        Set<T> result = new Set<>();

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

    public Set<T> EXCEPT(Set<T> set){
        Set<T> result = new Set<>();

        for(T key : this){
            if (!set.contains(key))
                result.add(key);
        }

        return result;
    }



}
