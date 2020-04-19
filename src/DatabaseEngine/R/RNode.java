package DatabaseEngine.R;

import DatabaseEngine.myPolygon;
import DatabaseEngine.BPlus.BPointer;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;

public abstract class RNode implements Serializable {  //B+ tree node
    private int size; //number of keys in the node


    private String ID;
    private int maxPerNode; //max number of keys the node can hold
    private int minPerNodes; //min number of keys the node can hold
    private  ArrayList<myPolygon> values; //keys

    RNode(int N, String ID){
        size = 0;
        maxPerNode = N;
        minPerNodes = N/2;
        this.ID = ID;
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

    public ArrayList<myPolygon> getValues() { //get key array
        return values;
    }

    public void setSize(int size){ //change size
        this.size = size;
    }

    public void setValues(ArrayList<myPolygon> values) { //set key array
        this.values = values;
    }

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    //------------------------METHODS-------------------------------

    public void incSize(){ //increase size
        size ++;
    }

    public void decSize(){ //decrease size
        size --;
    }

    public abstract RNode split(); //split node

    public abstract void delete(myPolygon value, BPointer p, String name);
    
    public void deleteNode() {

		try {
			String path =  "data//R//R_Nodes//" + "Node_" + ID + ".class";
			File f = new File(path);
			f.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
