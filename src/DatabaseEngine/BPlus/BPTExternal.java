package DatabaseEngine.BPlus;

import DatabaseEngine.Pointer;
import DatabaseEngine.Utilities;
import DatabaseEngine.overflowPage;

import java.io.File;
import java.util.ArrayList;
import java.util.Queue;

public class BPTExternal<T extends Comparable<T>> extends BPTNode<T> { //leaf

    private ArrayList<BPointer> pointers; //pointers to records
    private  String next; //next node in the linked list

    public BPTExternal(int N, String ID) {
        super(N,ID);
        pointers = new ArrayList<>();
        this.next = null;
    }

    // getters/setters:
    public String getNext(){
        return this.next;
    }

    public ArrayList<BPointer> getPointers(){
        return pointers;
    }

    public void setNext(String node){
        this.next = node;
    }

    public void setPointers(ArrayList<BPointer> pointers){
        this.pointers = pointers;
    }

    //------------------------METHODS-------------------------------

    public void insert(T value, BPointer newPointer) { //insert a value
        incSize();

        int insertAt = Utilities.selectiveBinarySearch(getValues(), value, ">="); //binary search for the correct place
        if (insertAt == -1) insertAt = getValues().size(); //inserted at the end

        getValues().add(insertAt, value); //insert and shift

        pointers.add(insertAt,newPointer); //insert record pointer
    }

    public BPTNode<T> split() {
        BPTExternal<T> newNode = new BPTExternal<>(getMaxPerNode(),""); //last node to the right
        ArrayList<T> newValues = new ArrayList<>(); //temp values
        ArrayList<BPointer> newPointers = new ArrayList<>(); //temp pointers

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

        return newNode; //node to be inserted as a pointer one level up the tree
    }
    
	public void delete(T value, BPointer temp,String name) {
		boolean flag = false;
		int	key = Utilities.selectiveBinarySearch(this.getValues(), value, "=");
		if(key != -1) {
			if(getPointers().get(key).equals(temp)) {
				getValues().remove(key);
				pointers.remove(key);
	            if (new File("data//BPlus//overflow_Pages//" + "overflow_" + name + value + "_0.class").isFile()){ //has overflow pages
	                overflowPage curPage = Utilities.deserializeOverflow(name + value + "_0"); //get the first page
	           		getPointers().add(key,(BPointer)curPage.poll());
            		getValues().add(key, value);
            		Utilities.serializeOverflow(curPage);   
	            }
        		Utilities.serializeNode(this);
			}
				
			else if (new File("data//BPlus//overflow_Pages//" + "overflow_" + name + value + "_0.class").isFile()){ //has overflow pages
                overflowPage curPage = Utilities.deserializeOverflow(name + value + "_0"); //get the first page

                while (curPage != null){ //loop over all overflow pages

                    Queue<Pointer> pointersQ = curPage.getPointers(); //get all pointers

                    while (!pointersQ.isEmpty()){ //for each pointer in page
                        BPointer p = (BPointer) pointersQ.poll();

                        if (p.compareTo(temp) == 0 ) {
                           curPage.deletePointer(p);
                           Utilities.serializeOverflow(curPage);  
                           flag = true;    
                           break;
                    }
                    }
                    if(flag) {
                    	break;
                    }
                    curPage = Utilities.deserializeOverflow(curPage.getNext()); //next page
                }
            }
		}	
	}

}
