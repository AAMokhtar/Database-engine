package DatabaseEngine.BPlus;

import DatabaseEngine.DBAppException;
import DatabaseEngine.Utilities;
import DatabaseEngine.index;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
public class BPlusTree<T extends Comparable<T>> implements Serializable,index<T> {

    private  String name; // tree filename (tablename + column name)
    private int maxPerNode; //max elements per node
    private int minPerNode; // min elements per node
    private BPTNode<T> root; //root node
    private ArrayList<pointer> pointerList; //all record pointers in tree sorted by page#, index#


    BPlusTree(String name, int N) throws DBAppException {
        this.name = name;
        maxPerNode = N;
        minPerNode = N/2;
        pointerList = new ArrayList<>();
    }

// getters/setters:
    public String getName(){
    return name;
}

    public int getMaxPerNode(){
        return maxPerNode;
    }

    public int getMinPerNode(){
        return minPerNode;
    }

    public ArrayList<pointer> getPointerList(){ return pointerList;}

//------------------------METHODS-------------------------------

    //-------------BASE METHODS-------------

    //Insert
    public void insert(T value, pointer recordPointer) throws DBAppException {
        shiftPointersAt(recordPointer.getPage(), recordPointer.getOffset(), 1); //shift old pointers
        getPointerList().add(recordPointer); //insert new pointer
        Collections.sort(pointerList); //sort

        insertHelp(value, recordPointer, root); //current node needed for recursion
    }

    //Search
    public ArrayList<pointer> search(T key){

        //TODO: i am almost dead
        return null;
    }


    //Delete
    public void delete(T key){
        //TODO: ALI'S PART
    }

    //-------------HELPERS-------------
    private BPTNode<T> insertHelp(T value, pointer recordPointer, BPTNode<T> cur) throws DBAppException {

        //create root (empty tree)
        if (cur == null){
            root = new BPTExternal<>(maxPerNode); //new leaf
            ((BPTExternal<T>) root).insert(value, recordPointer);  //insert in leaf
            return null;
        }
        
        ArrayList<T> values = cur.getValues(); //get node keys
        BPTNode<T> newPointer = null; //return value

        //----------------Down the tree (searching)-----------------

        if (cur instanceof BPTInternal){ //internal node (finding the leaf)
            ArrayList<BPTNode<T>> pointers = ((BPTInternal<T>) cur).getPointers(); //node pointers
            
            int index = Utilities.selectiveBinarySearch(values, value, "<="); //pointer index - 1

            newPointer = insertHelp(value, recordPointer, pointers.get(index + 1)); //recursion (go down one level)
        }

        //----------------Up the tree (inserting)-------------------

        //INSERTING IN LEAVES:
        if (cur instanceof BPTExternal ) {

            ((BPTExternal<T>) cur).insert(value, recordPointer); //insert value

            if (cur.getSize() > maxPerNode){ //node is full

                if (root == cur){ //we are at the root

                    root = new BPTInternal<>(getMaxPerNode()); //create node
                    ArrayList<BPTNode<T>> rootPointers =  ((BPTInternal<T>) root).getPointers();
                    rootPointers.add(cur); // add left child
                    rootPointers.add(cur.split()); // add right child
                    root.getValues().add(rootPointers.get(1).getValues().get(0)); //add splitting value
                    root.incSize();
                }

                else { //non root: split and take step back in the recursion tree
                    return cur.split();
                }
            }


            return null; //value added without splitting
        }

        //INSERTING IN INTERNAL NODES:
        else {

            if (newPointer == null) return null; //nothing to be inserted

            ((BPTInternal<T>) cur).insert(newPointer.getValues().get(0), newPointer); //insert value passed from child

            if (newPointer instanceof BPTInternal){ //if child is non leaf
                newPointer.decSize();
                newPointer.getValues().remove(0); //remove first element from child (a duplicate index)
            }

            if (cur.getSize() > maxPerNode){ //node is full after insertion

                if (root == cur){ //at root

                    root = new BPTInternal<>(getMaxPerNode()); //create node
                    ArrayList<BPTNode<T>> rootPointers =  ((BPTInternal<T>) root).getPointers();
                    rootPointers.add(cur); // add left child
                    rootPointers.add(cur.split()); // add right child
                    root.getValues().add(rootPointers.get(1).getValues().get(0)); //add splitting value
                    root.incSize();

                    rootPointers.get(1).getValues().remove(0); //remove first element from child (a duplicate index)
                    rootPointers.get(1).decSize();
                }

                else { //non root: split and take step back in the recursion tree
                    return cur.split();
                }
            }

            return null; //value added without splitting
        }
    }

    //shift pointers left or right
    public void shiftPointersAt(int pageNum, int vecIndex, int ammount){
        pointer temp = new pointer(pageNum,vecIndex); //to compare with other pointers

        //get max entries per page
        int maxTuplesPerPage = Integer.parseInt(Utilities.readProperties("config//DBApp.properties")
                .getProperty("MaximumRowsCountinPage"));

        int start = Utilities.selectiveBinarySearch(pointerList, temp,">=");

        for (int i = start; i < pointerList.size(); i++) {
            int offset = pointerList.get(i).getOffset(); //get index
            int page = pointerList.get(i).getPage(); //get page number


            offset = ((offset + ammount) % (maxTuplesPerPage + 1)); //index in new page
            pointerList.get(i).setOffset(offset); //set new index
            pointerList.get(i).setPage(page + (offset / maxTuplesPerPage)); //new page number

        }
    }
}
