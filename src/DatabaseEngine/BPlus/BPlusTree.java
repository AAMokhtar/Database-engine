package DatabaseEngine.BPlus;

import DatabaseEngine.*;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Queue;

public class BPlusTree<T extends Comparable<T>> implements index<T>, Serializable {

    private  String name; // tree filename (tablename + column name)
    private int maxPerNode; //max elements per node
    private long nodeID; //don't tell me there's not enough IDs!!!
    private int minPerNode; // min elements per node
    private BPTNode<T> root; //root node

    public BPlusTree(String name, int N) {
        this.name = name;
        maxPerNode = N;
        minPerNode = N/2; //change it as you see fit
        nodeID = 0; //i know it's already 0. this looks cleaner though.
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

    public void setNodeID(long size){
        nodeID = size;
    }

//------------------------METHODS-------------------------------

    //-------------BASE METHODS-------------

    //Insert
    public void insert(T value, pointer recordPointer, boolean changeOldPointers) throws DBAppException {
        if (changeOldPointers)
            shiftPointersAt(recordPointer.getPage(), recordPointer.getOffset(), 1); //if you inserted a new record as well

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
                index = Utilities.selectiveBinarySearch(curNode.getValues(), value, "="); //find the index of value

                if (curNode != null && index != -1){ // find all records = value
                       ret.add(curNode.getPointers().get(index)); //add pointer to output

                    if (new File("data//BPlus//overflow_Pages//" + "overflow_" + name + value + "_0.class").isFile()){
                        overflowPage curPage = Utilities.deserializeBOverflow(name + value + "_0"); //get the first page

                        while (curPage != null){ //loop over all overflow pages

                            Queue<pointer> pointers = curPage.getPointers();
                            while (!pointers.isEmpty()) ret.add(pointers.poll()); //add all pointers in page

                            curPage = Utilities.deserializeBOverflow(curPage.getNext()); //get next page

                        }

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
                        ret.add(curNode.getPointers().get(index)); // add to output

                        if (new File("data//BPlus//overflow_Pages//" + "overflow_" + name + curNode.getValues().get(index) + "_0.class").isFile()){
                            overflowPage curPage = Utilities.deserializeBOverflow(name + curNode.getValues().get(index) + "_0"); //get the first page

                            while (curPage != null){ //loop over all overflow pages

                                Queue<pointer> pointers = curPage.getPointers();
                                while (!pointers.isEmpty()) ret.add(pointers.poll()); //add pointers

                                curPage = Utilities.deserializeBOverflow(curPage.getNext()); //next page

                            }

                        }
                        index++;
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
                        ret.add(curNode.getPointers().get(index)); //add pointer to output

                        if (new File("data//BPlus//overflow_Pages//" + "overflow_" + name + curNode.getValues().get(index) + "_0.class").isFile()){
                            overflowPage curPage = Utilities.deserializeBOverflow(name + curNode.getValues().get(index) + "_0"); //get the first page

                            while (curPage != null){ //loop over all overflow pages

                                Queue<pointer> pointers = curPage.getPointers();
                                while (!pointers.isEmpty()) ret.add(pointers.poll()); //add pointers

                                curPage = Utilities.deserializeBOverflow(curPage.getNext()); //next page

                            }

                        }
                        index++;
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
                        ret.add(curNode.getPointers().get(index)); //add pointer to output

                        if (new File("data//BPlus//overflow_Pages//" + "overflow_" + name + curNode.getValues().get(index) + "_0.class").isFile()){
                            overflowPage curPage = Utilities.deserializeBOverflow(name + curNode.getValues().get(index++) + "_0"); //get the first page

                            while (curPage != null){ //loop over all overflow pages

                                Queue<pointer> pointers = curPage.getPointers();
                                while (!pointers.isEmpty()) ret.add(pointers.poll()); //add pointers

                                curPage = Utilities.deserializeBOverflow(curPage.getNext()); //next page

                            }

                        }
                        index++;
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
                        ret.add(curNode.getPointers().get(index)); //add pointer to output

                        if (new File("data//BPlus//overflow_Pages//" + "overflow_" + name + curNode.getValues().get(index) + "_0.class").isFile()){
                            overflowPage curPage = Utilities.deserializeBOverflow(name + curNode.getValues().get(index) + "_0"); //get the first page

                            while (curPage != null){ //loop over all overflow pages

                                Queue<pointer> pointers = curPage.getPointers();
                                while (!pointers.isEmpty()) ret.add(pointers.poll()); //add pointers

                                curPage = Utilities.deserializeBOverflow(curPage.getNext()); //next page

                            }

                        }
                        index++;
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
                        ret.add(curNode.getPointers().get(index)); //add to output

                        if (new File("data//BPlus//overflow_Pages//" + "overflow_" + name + curNode.getValues().get(index) + "_0.class").isFile()){
                            overflowPage curPage = Utilities.deserializeBOverflow(name + curNode.getValues().get(index) + "_0"); //get the first page

                            while (curPage != null){ //loop over all overflow pages

                                Queue<pointer> pointers = curPage.getPointers();
                                while (!pointers.isEmpty()) ret.add(pointers.poll()); //add pointers

                                curPage = Utilities.deserializeBOverflow(curPage.getNext()); //next page

                            }

                        }
                        index++;
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
        //TODO
    }

    //-------------HELPERS-------------
    private BPTNode<T> insertHelp(T value, pointer recordPointer, BPTNode<T> cur){

        //create root (empty tree)
        if (cur == null){
            root = new BPTExternal<>(maxPerNode, name + nodeID++); //new leaf (also root)
            ((BPTExternal<T>) root).insert(value, recordPointer);  //insert in leaf
            Utilities.serializeNode(root); //save root
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

            int index = Utilities.selectiveBinarySearch(cur.getValues(),value,"="); //search for the value inside node

            if (index == -1) //not a duplicate. insert into the node
                 ((BPTExternal<T>) cur).insert(value, recordPointer); //insert value
            else { //insert into an overflow page
                Utilities.overflowInsert(name,value,recordPointer);
            }

            if (cur.getSize() > maxPerNode){ //node is full

                if (root == cur){ //we are at the root

                    root = new BPTInternal<>(getMaxPerNode(),name + nodeID++); //create root
                    ArrayList<String> rootPointers =  ((BPTInternal<T>) root).getPointers();
                    rootPointers.add(cur.getID()); // add left child

                    BPTNode<T> rightChild = cur.split();
                    rightChild.setID(name + nodeID++); //give an ID to the new node
                    ((BPTExternal<T>) cur).setNext(rightChild.getID()); //current node points to child

                    rootPointers.add(rightChild.getID()); // add right child


                    root.getValues().add(rightChild.getValues().get(0)); //add splitting value
                    root.incSize();

                    Utilities.serializeNode(rightChild); //save node
                    Utilities.serializeNode(root); //save root
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
                    Utilities.serializeNode(root); //save root
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

        BPTExternal<T> cur = Utilities.findLeaf(root,null,true); //get the leftmost leaf

        while (cur != null){ //for all leaves
            ArrayList<pointer> pointers = cur.getPointers();
            ArrayList<T> values = cur.getValues();

            for(pointer p: pointers){
                if (p.compareTo(temp) >= 0) { //tuple's position is greater than or equal the given position

                    int offset = p.getOffset(); //get index
                    int page = p.getPage(); //get page number

                    if (amount >= 0) {
                        p.setPage(page + ((offset + amount) / maxTuplesPerPage)); //new page number
                        offset = ((offset + amount) % (maxTuplesPerPage)); //index in new page
                        p.setOffset(offset); //set new index
                    }
                    else if (p.getPage() == temp.getPage()){
                        offset = ((offset + amount) % (maxTuplesPerPage)); //index in new page
                        p.setOffset(offset); //set new index
                    }
                }

            }

            for(T v: values){ //get the overflow pages of every value

                if (new File("data//BPlus//overflow_Pages//" + "overflow_" + name + v + "_0.class").isFile()){ //has overflow pages
                    overflowPage curPage = Utilities.deserializeBOverflow(name + v + "_0"); //get the first page

                    while (curPage != null){ //loop over all overflow pages

                        Queue<pointer> pointersQ = curPage.getPointers(); //get all pointers

                        while (!pointersQ.isEmpty()){ //for each pointer in page
                            pointer p = pointersQ.poll();

                            if (p.compareTo(temp) >= 0) { //tuple's position is greater than or equal the given position
                                int offset = p.getOffset(); //get index
                                int page = p.getPage(); //get page number

                                if (amount >= 0) {
                                    p.setPage(page + ((offset + amount) / maxTuplesPerPage)); //new page number
                                    offset = ((offset + amount) % (maxTuplesPerPage)); //index in new page
                                    p.setOffset(offset); //set new index
                                }
                                else if (p.getPage() == temp.getPage()){
                                    offset = ((offset + amount) % (maxTuplesPerPage)); //index in new page
                                    p.setOffset(offset); //set new index
                                }
                            }
                        }
                        Utilities.serializeBOverflow(curPage);
                        curPage = Utilities.deserializeBOverflow(curPage.getNext()); //next page

                    }

                }
            }

            Utilities.serializeNode(cur);
            cur = (BPTExternal<T>) Utilities.deserializeNode(cur.getNext());
        }
    }

    public String leavesToString(){
        StringBuilder ret = new StringBuilder();

        BPTExternal cur = Utilities.findLeaf(root,null,true);

        while (cur != null){
            ret.append("[");

            int i = 0;
            for(Object val : cur.getValues()){
                ret.append(" " + val.toString());

                if (i != maxPerNode - 1) ret.append(" |");
            }

            while (i < maxPerNode - 1){
                ret.append("    |");
            }
            ret.append("    ");

            if (cur.getNext() != null) ret.append("] -> ");
            else ret.append("]");

            cur = (BPTExternal) Utilities.deserializeNode(cur.getNext());
        }
        return ret.toString();
    }
}
