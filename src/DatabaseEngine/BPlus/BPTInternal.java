package DatabaseEngine.BPlus;

import DatabaseEngine.Utilities;

import java.util.ArrayList;

public class BPTInternal<T extends Comparable<T>> extends BPTNode<T> { // non leaf nodes
    private ArrayList<String> pointers; //pointers to children nodes

    BPTInternal(int N, String ID) {
        super(N,ID);
        pointers = new ArrayList<>();
    }

    // getters/setters:
    public ArrayList<String> getPointers(){
        return pointers;
    }
    public void setPointers(ArrayList<String> pointers){
        this.pointers = pointers;
    }

    //------------------------METHODS-------------------------------

    public void insert(T value, String newPointer) {
        incSize();

        int insertAt = Utilities.selectiveBinarySearch(getValues(), value, ">="); //binary search for the correct place

        if (insertAt == -1) insertAt = getValues().size(); //inserted at the end
        getValues().add(insertAt, value); //insert and shift

        pointers.add(insertAt + 1,newPointer); //insert child pointer
    }

    public BPTNode<T> split() {
        BPTInternal<T> newNode = new BPTInternal<>(getMaxPerNode(),""); ////last node to the right (in the same level)
        ArrayList<T> newValues = new ArrayList<>(); //temp values
        ArrayList<String> newPointers = new ArrayList<>(); //temp pointers

        int splittingIndex = (getSize() - 1)/2; //split node in half

        while(getValues().size() - 1 > splittingIndex) { //add values to new node

            //-------New Node---------
            newValues.add(getValues().get(splittingIndex + 1));
            newPointers.add(getPointers().get(splittingIndex + 2));
            newNode.incSize();

            //-------Old Node---------
            getValues().remove(splittingIndex + 1);
            getPointers().remove(splittingIndex + 2);
            decSize();
        }

        newNode.setValues(newValues); //assign node values to temp array
        newNode.setPointers(newPointers); //assign child pointers to temp array

        return newNode; //node to be inserted as a pointer one level up the tree
    }
}
