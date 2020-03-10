package DatabaseEngine.BPlus;

import java.util.ArrayList;

public abstract class BPTNode<T extends Comparable<T>>{ //B+ tree node
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
    public int getMaxPerNode(){
        return maxPerNode;
    } // get the maximum number of keys the node can hold

    public int getMinPerNodes(){
        return minPerNodes;
    } // get the minimum number of keys the node can hold

    public int getSize(){
        return size;
    } //return size

    public ArrayList<T> getValues() {
        return values;
    } //get key array

    public void setSize(int size){ //change size
        this.size = size;
    }

    public void setValues(ArrayList<T> values) {
        this.values = values;
    } //set key array

    //------------------------METHODS-------------------------------

    public void incSize(){
        size ++;
    } //increase size

    public void decSize(){
        size --;
    }  //decrease size

    public abstract BPTNode<T> split(); //split node

}
