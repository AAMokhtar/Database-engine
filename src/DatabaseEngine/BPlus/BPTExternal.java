package DatabaseEngine.BPlus;

import java.util.ArrayList;

public class BPTExternal<T extends Comparable<T>> extends BPTNode<T> {

    private ArrayList<pointer> pointers;
    private  BPTExternal<T> next;

    BPTExternal(int N) {
        super(N);
        pointers = new ArrayList<>();
        this.next = null;
    }


    public BPTExternal<T> getNext(){
        return this.next;
    }

    public void setNext(BPTExternal<T> node){
        this.next = node;
    }

    public ArrayList<pointer> getPointers(){
        return pointers;
    }



}
