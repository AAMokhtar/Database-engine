package DatabaseEngine.BPlus;

import DatabaseEngine.BPlus.BPTNode;

import java.util.ArrayList;

public class BPTInternal<T extends Comparable<T>> extends BPTNode<T> {
    private ArrayList<BPTNode<T>> pointers;

    BPTInternal(int N) {
        super(N);
        pointers = new ArrayList<>();
    }

    public ArrayList<BPTNode<T>> getPointers(){
        return pointers;
    }

}
