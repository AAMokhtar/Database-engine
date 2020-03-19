package DatabaseEngine.BPlus;

import DatabaseEngine.*;

import java.io.Serializable;
import java.util.ArrayList;

public class BPlusTree<T extends Comparable<T>> implements index<T>, Serializable {

    private  String name; // tree filename (tablename + column name)
    private int maxPerNode; //max elements per node
    private long nodeID; //don't tell me there's not enough IDs!!!
    private int minPerNode; // min elements per node
    private BPTNode<T> root; //root node
    private ArrayList<pointer> pointerList; //all record pointers in tree sorted by page#, index#


    public BPlusTree(String name, int N) {
        this.name = name;
        maxPerNode = N;
        minPerNode = N/2; //change it as you see fit
        nodeID = 0; //i know it's already 0. this looks cleaner though.
        pointerList = new ArrayList<>();
    }

// getters/setters:
    public String getName(){
    return name;
}

    public int getMaxPerNode(){
        return maxPerNode;
    }

    public long getNodeID(){
        return nodeID;
    }

    public int getMinPerNode(){
        return minPerNode;
    }

    public ArrayList<pointer> getPointerList(){ return pointerList;}

    public void setNodeID(long size){
        nodeID = size;
    }

//------------------------METHODS-------------------------------

    //-------------BASE METHODS-------------

    //Insert
    public void insert(T value, pointer recordPointer, boolean changeOldPointers) throws DBAppException {
        if (changeOldPointers)
            shiftPointersAt(recordPointer.getPage(), recordPointer.getOffset(), 1); //if you inserted a new record as well

        int pointerPlace = Utilities.selectiveBinarySearch(getPointerList(), recordPointer, ">="); //get correct position

        if(pointerPlace == -1) pointerPlace = getPointerList().size(); //insert at the end

        getPointerList().add(pointerPlace,recordPointer); //insert pointer

        insertHelp(value, recordPointer, root); //the actual insertion
    }

    //Search
    public BSet<pointer> search(T value, String operator){

        BSet<pointer> ret = new BSet<>(); //output array
        BPTExternal<T> curNode;
        int index;

        switch (operator){

            case "=":
                curNode = Utilities.findLeaf(root, value, false); //find node of value
                index = Utilities.selectiveBinarySearch(curNode.getValues(), value, "="); //find first index with value

                while (curNode != null && index != -1){ // find all records = value

                    if (index == curNode.getValues().size()){ //next node
                        curNode = (BPTExternal<T>) Utilities.deserializeNode(curNode.getNext());
                        index = 0;

                    }

                    else if (curNode.getValues().get(index) != value){ // record > value
                        break;
                    }

                    else {
                       ret.add(curNode.getPointers().get(index++)); //add pointer to output
                    }
                }

                break;

            //-----------------NEXT CASE--------------------

            case "!=":

                curNode = Utilities.findLeaf(root, value, true); //first node
                index = 0;

                while (curNode != null){ // find all records != value

                    if (index == curNode.getValues().size()){ //next node
                        curNode = (BPTExternal<T>) Utilities.deserializeNode(curNode.getNext());
                        index = 0;

                    }

                    else if (curNode.getValues().get(index) != value){
                        ret.add(curNode.getPointers().get(index++)); // add to output
                    }
                }

                break;

            //-----------------NEXT CASE--------------------

            case ">=":

                curNode = Utilities.findLeaf(root, value, false); //find node
                index = Utilities.selectiveBinarySearch(curNode.getValues(), value, "<") + 1; //filter <

                while (curNode != null){ // find all records >= value

                    if (index == curNode.getValues().size()){ //next node
                        curNode = (BPTExternal<T>) Utilities.deserializeNode(curNode.getNext());
                        index = 0;

                    }
                    else {
                        ret.add(curNode.getPointers().get(index++)); //add pointer to output
                    }
                }

                break;

            //-----------------NEXT CASE--------------------

            case ">":

                curNode = Utilities.findLeaf(root, value, false); //get node that contains value
                index = Utilities.selectiveBinarySearch(curNode.getValues(), value, "<=") + 1; //filter <=

                while (curNode != null){ // find all records < value

                    if (index == curNode.getValues().size()){ //next node
                        curNode = (BPTExternal<T>) Utilities.deserializeNode(curNode.getNext());
                        index = 0;

                    }
                    else {
                        ret.add(curNode.getPointers().get(index++)); //add pointer to output
                    }
                }

                break;

            //-----------------NEXT CASE--------------------

            case "<=":

                curNode = Utilities.findLeaf(root, value, true); //first node
                index = 0;

                while (curNode != null){ // find all records <= value

                    if (index == curNode.getValues().size()){ //next node
                        curNode = (BPTExternal<T>) Utilities.deserializeNode(curNode.getNext());
                        index = 0;

                    }

                    else if (curNode.getValues().get(index).compareTo(value) > 0){ //record greater than value
                        break;
                    }

                    else {
                        ret.add(curNode.getPointers().get(index++)); //add pointer to output
                    }
                }

                break;

            //-----------------NEXT CASE--------------------

            case "<":

                curNode = Utilities.findLeaf(root, value, true); //first element
                index = 0;

                while (curNode != null){ // find all records < value

                    if (index == curNode.getValues().size()){ //next mode
                        curNode = (BPTExternal<T>) Utilities.deserializeNode(curNode.getNext());
                        index = 0;

                    }

                    else if (curNode.getValues().get(index).compareTo(value) >= 0){ // record > value
                        break;
                    }
                    else {
                        ret.add(curNode.getPointers().get(index++)); //add to output
                    }
                }

                break;

            //-----------------NEXT CASE--------------------

            default: break;
        }

        return ret;
    }

    //Delete
    public void delete(T key){
        //TODO: ALI'S PART
    }

    //-------------HELPERS-------------
    private BPTNode<T> insertHelp(T value, pointer recordPointer, BPTNode<T> cur){

        //create root (empty tree)
        if (cur == null){
            root = new BPTExternal<>(maxPerNode, name + nodeID++); //new leaf (also root)
            ((BPTExternal<T>) root).insert(value, recordPointer);  //insert in leaf
            return null;
        }
        
        ArrayList<T> values = cur.getValues(); //get node keys
        BPTNode<T> newPointer = null; //value to be returned (only needed for recursion)

        //----------------Down the tree (searching)-----------------

        if (cur instanceof BPTInternal){ //internal node (finding the leaf)
            ArrayList<String> pointers = ((BPTInternal<T>) cur).getPointers(); //node pointers
            
            int index = Utilities.selectiveBinarySearch(values, value, "<="); //pointer index - 1
            BPTNode<T> dNode = Utilities.deserializeNode(pointers.get(index + 1)); //deserialize the child

            newPointer = insertHelp(value, recordPointer, dNode); //recursion (go down one level)
        }

        //----------------Up the tree (inserting)-------------------

        //INSERTING IN LEAVES:
        if (cur instanceof BPTExternal ) {

            ((BPTExternal<T>) cur).insert(value, recordPointer); //insert value

            if (cur.getSize() > maxPerNode){ //node is full

                if (root == cur){ //we are at the root

                    root = new BPTInternal<>(getMaxPerNode(),name + nodeID++); //create root
                    ArrayList<String> rootPointers =  ((BPTInternal<T>) root).getPointers();
                    rootPointers.add(cur.getID()); // add left child

                    BPTNode<T> rightChild = cur.split();
                    rightChild.setID(name + nodeID++); //give an ID to the new node
                    Utilities.serializeNode(rightChild); //save node
                    ((BPTExternal<T>) cur).setNext(rightChild.getID()); //current node points to child

                    rootPointers.add(rightChild.getID()); // add right child


                    root.getValues().add(rightChild.getValues().get(0)); //add splitting value
                    root.incSize();
                }

                else { //non root: split and take step back in the recursion tree
                    BPTNode<T> node = cur.split(); //return node after assigning its ID
                    node.setID(name + nodeID++);
                    Utilities.serializeNode(node); //save node
                    Utilities.serializeNode(cur); //save node
                    return node;
                }
            }

            Utilities.serializeNode(cur); //save node
            return null; //value added without splitting
        }

        //INSERTING IN INTERNAL NODES:
        else {

            if (newPointer == null) return null; //nothing to be inserted

            ((BPTInternal<T>) cur).insert(newPointer.getValues().get(0), newPointer.getID()); //insert value passed from child

            if (newPointer instanceof BPTInternal){ //if child is non leaf
                newPointer.decSize();
                newPointer.getValues().remove(0); //remove first element from child (a duplicate index)
                Utilities.serializeNode(newPointer); //save node
            }

            if (cur.getSize() > maxPerNode){ //node is full after insertion

                if (root == cur){ //at root

                    root = new BPTInternal<>(getMaxPerNode(), name + nodeID++); //create node
                    ArrayList<String> rootPointers =  ((BPTInternal<T>) root).getPointers();
                    rootPointers.add(cur.getID()); // add left child

                    BPTNode<T> rightChild = cur.split();
                    rightChild.setID(name + nodeID++); //give an ID to the new node

                    rootPointers.add(rightChild.getID()); // add right child
                    root.getValues().add(rightChild.getValues().get(0)); //add splitting value
                    root.incSize();

                    rightChild.getValues().remove(0); //remove first element from child (a duplicate index)
                    rightChild.decSize();

                    Utilities.serializeNode(rightChild); //save right child
                }

                else { //non root: split and take step back in the recursion tree
                    BPTNode<T> node = cur.split(); //return node after assigning its ID
                    node.setID(name + nodeID++);
                    Utilities.serializeNode(node); //save node
                    Utilities.serializeNode(cur); //save node
                    return node;
                }
            }

            Utilities.serializeNode(cur); //save node
            return null; //value added without splitting
        }
    }

    //shift pointers left or right
    public void shiftPointersAt(int pageNum, int vecIndex, int amount){
        pointer temp = new pointer(pageNum,vecIndex); //to compare with other pointers

        //get max entries per page
        int maxTuplesPerPage = Integer.parseInt(Utilities.readProperties("config//DBApp.properties")
                .getProperty("MaximumRowsCountinPage"));

        BPTExternal<T> cur = Utilities.findLeaf(root,null,true);
        while (cur != null){
            ArrayList<pointer> pointers = cur.getPointers();

            for(pointer p: pointers){
                int offset = p.getOffset(); //get index
                int page = p.getPage(); //get page numberf

                //the offset became negative after shifting
                int negative = ((offset + amount < 0) && (offset + amount)%maxTuplesPerPage != 0?-1:0);

                p.setPage(page + ((offset + amount) / maxTuplesPerPage) + negative); //new page number
                offset = ((offset + amount) % (maxTuplesPerPage)); //index in new page
                p.setOffset(offset); //set new index

            }

            Utilities.serializeNode(cur);
            cur = (BPTExternal<T>) Utilities.deserializeNode(cur.getNext());
        }

    }
}
