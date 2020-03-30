package DatabaseEngine.BPlus;

import DatabaseEngine.*;
import javafx.util.Pair;

import java.io.File;
import java.io.Serializable;
import java.util.*;

public class BPlusTree<T extends Comparable<T>> implements index<T>, Serializable {

    private  String name; // tree filename (tablename + column name)
    private  String table; // table name (needed for shifting)
    private String column; //table column needed for name along with the table name
    private int maxPerNode; //max elements per node
    private long nodeID; //don't tell me there's not enough IDs!!!
    private int minPerNode; // min elements per node
    private BPTNode<T> root; //root node

    public BPlusTree(String table,String column) {
        this.column = column;
        this.table = table;
        this.name = table +"_"+ column;
        maxPerNode = Utilities.readNodeSize("config//DBApp.properties"); //maximum node size loaded from properties
        minPerNode = maxPerNode/2; //change it as you see fit
        nodeID = 0; //i know it's already 0. this looks cleaner though.
    }

// getters/setters:
    public String getName(){
    return name;
}

    public int getMaxPerNode(){
        return maxPerNode;
    }

    public BPTNode<T> getRoot() { return root; }

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
    public void insert(T value, BPointer recordPointer, boolean changeOldPointers) throws DBAppException {
        if (changeOldPointers)
            incrementPointersAt(recordPointer.getPage(), recordPointer.getOffset()); //if you inserted a new record as well

        insertHelp(value, recordPointer, root); //the actual insertion
    }

    //Search
    public BSet<BPointer> search(T value, String operator){

        BSet<BPointer> ret = new BSet<>(); //output array
        BPTExternal<T> curNode;
        int index;

        switch (operator){

            case "=":
                curNode = Utilities.findLeaf(root, value, false); //find node of value
                index = Utilities.selectiveBinarySearch(curNode.getValues(), value, "="); //find the index of value

                if (curNode != null && index != -1){ // find all records = value
                       ret.add(curNode.getPointers().get(index)); //add pointer to output

                    String path = "data//overflow_Pages//" + "overflow_" + name +"_"+ value + "_0.class";
                    path = path.replaceAll("[^a-zA-Z0-9()_./+]",""); //windows is gay

                    if (new File(path).isFile()){
                        overflowPage curPage = Utilities.deserializeOverflow(name +"_"+ value + "_0"); //get the first page

                        while (curPage != null){ //loop over all overflow pages

                            Queue<Pointer> pointers = curPage.getPointers();
                            while (!pointers.isEmpty()) ret.add((BPointer) pointers.poll()); //add all pointers in page

                            curPage = Utilities.deserializeOverflow(curPage.getNext()); //get next page

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

                        String path = "data//overflow_Pages//" + "overflow_" + name +"_"+ curNode.getValues().get(index) + "_0.class";
                        path = path.replaceAll("[^a-zA-Z0-9()_./+]",""); //windows is gay

                        if (new File(path).isFile()){
                            overflowPage curPage = Utilities.deserializeOverflow(name +"_"+ curNode.getValues().get(index) + "_0"); //get the first page

                            while (curPage != null){ //loop over all overflow pages

                                Queue<Pointer> pointers = curPage.getPointers();
                                while (!pointers.isEmpty()) ret.add((BPointer) pointers.poll()); //add pointers

                                curPage = Utilities.deserializeOverflow(curPage.getNext()); //next page

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

                        String path = "data//overflow_Pages//" + "overflow_" + name +"_"+ curNode.getValues().get(index) + "_0.class";
                        path = path.replaceAll("[^a-zA-Z0-9()_./+]",""); //windows is gay

                        if (new File(path).isFile()){
                            overflowPage curPage = Utilities.deserializeOverflow(name +"_"+ curNode.getValues().get(index) + "_0"); //get the first page

                            while (curPage != null){ //loop over all overflow pages

                                Queue<Pointer> pointers = curPage.getPointers();
                                while (!pointers.isEmpty()) ret.add((BPointer) pointers.poll()); //add pointers

                                curPage = Utilities.deserializeOverflow(curPage.getNext()); //next page

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

                        String path = "data//overflow_Pages//" + "overflow_" + name +"_"+ curNode.getValues().get(index) + "_0.class";
                        path = path.replaceAll("[^a-zA-Z0-9()_./+]",""); //windows is gay

                        if (new File(path).isFile()){
                            overflowPage curPage = Utilities.deserializeOverflow(name +"_"+curNode.getValues().get(index++) + "_0"); //get the first page

                            while (curPage != null){ //loop over all overflow pages

                                Queue<Pointer> pointers = curPage.getPointers();
                                while (!pointers.isEmpty()) ret.add((BPointer) pointers.poll()); //add pointers

                                curPage = Utilities.deserializeOverflow(curPage.getNext()); //next page

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

                        String path = "data//overflow_Pages//" + "overflow_" + name +"_"+ curNode.getValues().get(index) + "_0.class";
                        path = path.replaceAll("[^a-zA-Z0-9()_./+]",""); //windows is gay

                        if (new File(path).isFile()){
                            overflowPage curPage = Utilities.deserializeOverflow(name +"_"+ curNode.getValues().get(index) + "_0"); //get the first page

                            while (curPage != null){ //loop over all overflow pages

                                Queue<Pointer> pointers = curPage.getPointers();
                                while (!pointers.isEmpty()) ret.add((BPointer) pointers.poll()); //add pointers

                                curPage = Utilities.deserializeOverflow(curPage.getNext()); //next page

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

                        String path = "data//overflow_Pages//" + "overflow_" + name +"_"+ curNode.getValues().get(index) + "_0.class";
                        path = path.replaceAll("[^a-zA-Z0-9()_./+]",""); //windows is gay

                        if (new File(path).isFile()){
                            overflowPage curPage = Utilities.deserializeOverflow(name +"_"+ curNode.getValues().get(index) + "_0"); //get the first page

                            while (curPage != null){ //loop over all overflow pages

                                Queue<Pointer> pointers = curPage.getPointers();
                                while (!pointers.isEmpty()) ret.add((BPointer) pointers.poll()); //add pointers

                                curPage = Utilities.deserializeOverflow(curPage.getNext()); //next page

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
    private BPTNode<T> insertHelp(T value, BPointer recordPointer, BPTNode<T> cur){

        //create root (empty tree)
        if (cur == null){
            root = new BPTExternal<>(maxPerNode, name +"_"+ nodeID++); //new leaf (also root)
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
                overflowPage.insert(name+"_"+value,recordPointer);
            }

            if (cur.getSize() > maxPerNode){ //node is full

                if (root == cur){ //we are at the root

                    root = new BPTInternal<>(getMaxPerNode(),name +"_"+ nodeID++); //create root
                    ArrayList<String> rootPointers =  ((BPTInternal<T>) root).getPointers();
                    rootPointers.add(cur.getID()); // add left child

                    BPTNode<T> rightChild = cur.split();
                    rightChild.setID(name +"_"+ nodeID++); //give an ID to the new node
                    ((BPTExternal<T>) cur).setNext(rightChild.getID()); //current node points to child

                    rootPointers.add(rightChild.getID()); // add right child


                    root.getValues().add(rightChild.getValues().get(0)); //add splitting value
                    root.incSize();

                    Utilities.serializeNode(rightChild); //save node
                    Utilities.serializeNode(root); //save root
                }

                else { //non root: split and take step back in the recursion tree
                    BPTNode<T> node = cur.split(); //return node after assigning its ID
                    node.setID(name +"_"+ nodeID++);
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

                    root = new BPTInternal<>(getMaxPerNode(), name +"_"+ nodeID++); //create node
                    ArrayList<String> rootPointers =  ((BPTInternal<T>) root).getPointers();
                    rootPointers.add(cur.getID()); // add left child

                    BPTNode<T> rightChild = cur.split();
                    rightChild.setID(name +"_"+ nodeID++); //give an ID to the new node

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
                    node.setID(name +"_"+ nodeID++);
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
    public void decrementPointersAt(int pageNum, int vecIndex){
        BPointer temp = new BPointer(pageNum,vecIndex); //to compare with other pointers

        BPTExternal<T> cur = Utilities.findLeaf(root,null,true); //get the leftmost leaf

        while (cur != null){ //for all leaves
            ArrayList<BPointer> pointers = cur.getPointers();
            ArrayList<T> values = cur.getValues();

            for(BPointer p: pointers){
                //tuple's position is greater than or equal the given position and it's in the same page
                if (p.compareTo(temp) >= 0 && p.getPage() == temp.getPage())
                    p.setOffset(p.getOffset() - 1); //set new index
            }

            for(T v: values){ //get the overflow pages of every value

                String path = "data//overflow_Pages//" + "overflow_" + name + "_" + v + "_0.class";
                path = path.replaceAll("[^a-zA-Z0-9()_./+]",""); //windows is gay

                if (new File(path).isFile()){ //has overflow pages
                    overflowPage curPage = Utilities.deserializeOverflow(name + "_" + v + "_0"); //get the first page

                    while (curPage != null){ //loop over all overflow pages

                        Queue<Pointer> pointersQ = curPage.getPointers(); //get all pointers

                        while (!pointersQ.isEmpty()){ //for each pointer in page
                            BPointer p = (BPointer) pointersQ.poll();

                            if (p.compareTo(temp) >= 0 && p.getPage() == temp.getPage())
                                p.setOffset(p.getOffset() - 1); //set new index

                        }

                        Utilities.serializeOverflow(curPage);
                        curPage = Utilities.deserializeOverflow(curPage.getNext()); //next page
                    }
                }
            }

            Utilities.serializeNode(cur);
            cur = (BPTExternal<T>) Utilities.deserializeNode(cur.getNext());
        }
    }

    public void incrementPointersAt(int pageNum, int vecIndex){
        Pair<HashMap<Integer,HashMap<Integer, BPointer>>,ArrayList<overflowPage>> temp =
                Utilities.getAllBPointers(this);
        HashMap<Integer,HashMap<Integer, BPointer>> pointers = temp.getKey(); //all the pointers of the tree
        ArrayList<overflowPage> overflow = temp.getValue(); //all the overflow pages of the tree

        //table page Ids
        Vector<Integer> pageIds = Utilities.deserializeTable(table).getPages();

        //get max entries per page
        int max = Utilities.readPageSize("config//DBApp.properties"); // maximum tuples per page

        int i = Utilities.selectiveBinarySearch(new ArrayList<>(pageIds),pageNum,"="); //starting page
        int j = vecIndex;
        while (i < pageIds.size()){ //every page after the starting page
            int curPageSize = Utilities.deserializePage(pageIds.get(i)).getElementsCount(); //get number of tuples in page
            while (j < curPageSize){ //all pointers
                BPointer curPointer = pointers.get(pageIds.get(i)).get(j);

                if (curPointer.getOffset() + 1  == max){ //last record in a full page
                    //make it the first record in the next page
                    curPointer.setPage (i + 1 == pageIds.size()?Utilities.readNextId("config//DBApp.properties"):pageIds.get(i + 1));
                    curPointer.setOffset(0);
                }
                else {
                    //shift it once to the right
                    curPointer.setOffset(j + 1);
                }

                j++;
            }

            if (curPageSize < max) break; //no need to shit subsequent pointers

            i++;
            j = 0;
        }
        Utilities.serializeAll(this,overflow);
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
