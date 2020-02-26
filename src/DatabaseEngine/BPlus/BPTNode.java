package DatabaseEngine.BPlus;

import java.util.ArrayList;

public abstract class BPTNode<T extends Comparable<T>>{
    private int size;
    private  ArrayList<T> values;

    BPTNode(int N){
        size = 0;
        values = new ArrayList<>();
    }

    public int getSize(){
        return size;
    }

    public void incSize(){
        size ++;
    }

    public void decSize(){
        size --;
    }

    public ArrayList<T> getValues() {
        return values;
    }

}
