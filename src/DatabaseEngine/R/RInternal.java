package DatabaseEngine.R;

import DatabaseEngine.BPlus.BPointer;
import DatabaseEngine.Utilities;
import DatabaseEngine.myPolygon;

import java.util.ArrayList;

public class RInternal extends RNode { // non leaf nodes
    private ArrayList<String> pointers; //pointers to children nodes

    RInternal(int N, String ID) {
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

    public void insert(myPolygon value, String newPointer) {
        incSize();

        int insertAt = Utilities.selectiveBinarySearch(getValues(), value, ">="); //binary search for the correct place

        if (insertAt == -1) insertAt = getValues().size(); //inserted at the end
        getValues().add(insertAt, value); //insert and shift

        pointers.add(insertAt + 1,newPointer); //insert child pointer
    }

    public RNode split() {
        RInternal newNode = new RInternal(getMaxPerNode(),""); ////last node to the right (in the same level)
        ArrayList<myPolygon> newValues = new ArrayList<>(); //temp values
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
	public void delete(myPolygon value, BPointer p, String name) {
		int key = Utilities.selectiveBinarySearch(this.getValues(), value, "<="); // find place in array (greatest index
																					// less than or equal value)
		int valueKey = key;
		if (valueKey == -1)
			valueKey = 0;

		RNode childnode = (Utilities.deserializeRNode(((RInternal) this).getPointers().get(key + 1)));
		if (childnode instanceof RInternal)
			((RInternal) childnode).delete(value, p, name);
		else
			((RExternal) childnode).delete(value, p, name);

		if (childnode.getValues().size() >= this.getMinPerNodes()) {
			if (key != -1) {
				this.getValues().set(valueKey, Utilities.findLeaf(childnode, null, true).getValues().get(0));
				Utilities.serializeRNode(this);
			}
		} else {

			if (key + 1 < this.getPointers().size() - 1 && key + 1 > 0) { // it has left and right siblings
				RNode leftnode = (Utilities.deserializeRNode(((RInternal) this).getPointers().get(key)));
				if (childnode.getValues().size() + leftnode.getValues().size() >= 2 * getMinPerNodes())
					RTree.borrowFromLeft(this, childnode, leftnode, valueKey);
				else
					RTree.mergeWithLeft(this, childnode, leftnode, valueKey);
			} else if (key + 1 < this.getPointers().size() - 1) {
				RNode rightnode = (Utilities.deserializeRNode(((RInternal) this).getPointers().get(key + 2)));
				if (childnode.getValues().size() + rightnode.getValues().size() >= 2 * getMinPerNodes())
					RTree.borrowFromRight(this, childnode, rightnode, valueKey);
				else
					RTree.mergeWithRight(this, childnode, rightnode, valueKey);
			} else if (key + 1 > 0) {
				RNode leftnode = (Utilities.deserializeRNode(((RInternal) this).getPointers().get(key)));
				if (childnode.getValues().size() + leftnode.getValues().size() >= 2 * getMinPerNodes())
					RTree.borrowFromLeft(this, childnode, leftnode, key);
				else
					RTree.mergeWithLeft(this, childnode, leftnode, key);
			}

		}
	}
}
