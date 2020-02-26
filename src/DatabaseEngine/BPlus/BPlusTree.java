package DatabaseEngine.BPlus;

import DatabaseEngine.DBAppException;
import DatabaseEngine.Utilities;

import java.io.Serializable;
import java.util.ArrayList;

public class BPlusTree<T extends Comparable<T>> implements Serializable {
    private  String name;
    private int maxNodes;
    private int minNodes;
    private BPTNode<T> root;


    BPlusTree(String name, int N) throws DBAppException {
        this.name = name;
        maxNodes = N;
        minNodes = N/2;
    }

    public int getMaxNodes(){
        return maxNodes;
    }

    public int getMinNodes(){
        return minNodes;
    }

    public void insert(T value,BPTNode<T> cur) throws DBAppException {

        //creating root
        if (cur == null){
            root = new BPTExternal<>(maxNodes);
            root.getValues().add(value);
            return;
        }
        //-----------------------------------------
        ArrayList<T> values = cur.getValues(); //node keys

        if (cur instanceof BPTInternal){
                ArrayList<BPTNode<T>> pointers = ((BPTInternal<T>) cur).getPointers(); //node pointers

        }


        //-----------------------------------------
        if (cur instanceof BPTExternal ) { //insert in a leaf

            if (cur.getSize() < maxNodes) { //CASE1: a vacant space
                cur.incSize();

                ArrayList<pointer> pointers =  ((BPTExternal<T>) cur).getPointers(); //node pointers

                int insertAt = Utilities.binarySearchLeastGreaterEq(values, value, ">="); //binary search for the correct place
                values.add(insertAt, value); //insert and shift

    //TODO      pointers.add(insertAt,new pointer(-1,-1)); //fix pointer

            }
            else { //CASE2: full node


            }
        }


        else { //inserting in an internal node

        }
    }
}
