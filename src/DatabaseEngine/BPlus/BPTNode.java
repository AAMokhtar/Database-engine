package DatabaseEngine.BPlus;

import java.io.Serializable;
import java.util.ArrayList;

public abstract class BPTNode<T extends Comparable<T>> implements Serializable {  //B+ tree node
    private int size; //number of keys in the node
    private int maxPerNode; //max number of keys the node can hold
    private int minPerNodes; //min number of keys the node can hold
    private  ArrayList<T> values; //keys

    BPTNode(int N){
        size = 0;
        maxPerNode = N;
        minPerNodes = N/2;
        values = new ArrayList<>();
    }

    // getters/setters:

    public int getMaxPerNode(){ // get the maximum number of keys the node can hold
        return maxPerNode;
    }

    public int getMinPerNodes(){ // get the minimum number of keys the node can hold
        return minPerNodes;
    }

    public int getSize(){ //return size
        return size;
    }

    public ArrayList<T> getValues() { //get key array
        return values;
    }

    public void setSize(int size){ //change size
        this.size = size;
    }

    public void setValues(ArrayList<T> values) { //set key array
        this.values = values;
    }

    //------------------------METHODS-------------------------------

    public void incSize(){ //increase size
        size ++;
    }

    public void decSize(){ //decrease size
        size --;
    }

    public abstract BPTNode<T> split(); //split node

}
