package DatabaseEngine.BPlus;

import DatabaseEngine.Utilities;
import DatabaseEngine.pointer;

import java.util.ArrayList;

public class BPTExternal<T extends Comparable<T>> extends BPTNode<T> { //leaf

    private ArrayList<pointer> pointers; //pointers to records
    private  BPTExternal<T> next; //next node in the linked list

    BPTExternal(int N) {
        super(N);
        pointers = new ArrayList<>();
        this.next = null;
    }

    // getters/setters:
    public BPTExternal<T> getNext(){
        return this.next;
    }

    public ArrayList<pointer> getPointers(){
        return pointers;
    }

    public void setNext(BPTExternal<T> node){
        this.next = node;
    }

    public void setPointers(ArrayList<pointer> pointers){
        this.pointers = pointers;
    }

    //------------------------METHODS-------------------------------

    public void insert(T value, pointer newPointer) { //insert a value
        incSize();

        int insertAt = Utilities.selectiveBinarySearch(getValues(), value, ">="); //binary search for the correct place
        if (insertAt == -1) insertAt = getValues().size(); //inserted at the end

        getValues().add(insertAt, value); //insert and shift

        pointers.add(insertAt,newPointer); //insert record pointer
    }

    public BPTNode<T> split() {
        BPTExternal<T> newNode = new BPTExternal<>(getMaxPerNode()); //last node to the right
        ArrayList<T> newValues = new ArrayList<>(); //temp values
        ArrayList<pointer> newPointers = new ArrayList<>(); //temp pointers

        int splittingIndex = (getSize() - 1)/2; //split node in half

        while(getValues().size() - 1 > splittingIndex) { //redistribute values and pointers

            //-------New Node---------
            newValues.add(getValues().get(splittingIndex + 1));
            newPointers.add(getPointers().get(splittingIndex + 1));
            newNode.incSize();

            //-------Old Node---------
            getValues().remove(splittingIndex + 1);
            getPointers().remove(splittingIndex + 1);
            decSize();

        }

        newNode.setValues(newValues); //assign temp values to new node
        newNode.setPointers(newPointers);  //assign temp pointers to new node

        //linked list pointer assignment
        newNode.setNext(this.next);
        this.setNext(newNode);

        return newNode; //node to be inserted as a pointer one level up the tree
    }
}
