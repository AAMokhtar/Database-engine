package DatabaseEngine.R;

import DatabaseEngine.Page;
import DatabaseEngine.Utilities;
import DatabaseEngine.myPolygon;
import DatabaseEngine.overflowPage;
import DatabaseEngine.BPlus.BPointer;

import java.io.File;
import java.util.ArrayList;

public class RExternal extends RNode { //leaf

    private ArrayList<BPointer> pointers; //pointers to records
    private  String next; //next node in the linked list

    public RExternal(int N, String ID) {
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

    public void insert(myPolygon value, BPointer newPointer) { //insert a value
        incSize();

        int insertAt = Utilities.selectiveBinarySearch(getValues(), value, ">="); //binary search for the correct place
        if (insertAt == -1) insertAt = getValues().size(); //inserted at the end

        getValues().add(insertAt, value); //insert and shift

        pointers.add(insertAt,newPointer); //insert record pointer
    }

    public RNode split() {
        RExternal newNode = new RExternal(getMaxPerNode(),""); //last node to the right
        ArrayList newValues = new ArrayList<>(); //temp values
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

	public void delete(myPolygon value, BPointer temp, String name) {
		boolean flag = false;
		int	key = Utilities.selectiveBinarySearch(this.getValues(), value, "=");
		if(key != -1) {
			if(getPointers().get(key).equals(temp)) {
				getValues().remove(key);
				pointers.remove(key);
				decSize();

                String path = "data//overflow_Pages//" + "overflow_" + name +"_" + value + "_0.class" ;
                path = path.replaceAll("[^a-zA-Z0-9()_./+]","");

                if (new File(path).isFile()){ //has overflow pages
					overflowPage curPage = Utilities.deserializeOverflow(name + "_"+value + "_0"); //get the first page

	            	getPointers().add(key,(BPointer)curPage.poll());

                    Page recordPage = Utilities.deserializePage(getPointers().get(key).getPage());
                    String[] table_column = name.split("_");
                    int columnIndex = Utilities.getColumnFromMetadata(table_column[1]
                            ,Utilities.readMetaDataForSpecificTable(table_column[0]))
                            .getValue();

            		getValues().add(key, (myPolygon) recordPage.getPageElements().
                            get(getPointers().get(key).getOffset()).get(columnIndex));
            		incSize();
            		if(curPage.size()!=0)
            		Utilities.serializeOverflow(curPage);   

	            }
        		Utilities.serializeRNode(this);
			}
				
			else {
                String path = "data//overflow_Pages//" + "overflow_" + name + "_" + value + "_0.class";
                path = path.replaceAll("[^a-zA-Z0-9()_./+]","");
                if (new File(path).isFile()) {
                    //has overflow pages
                    overflowPage curPage = Utilities.deserializeOverflow(name + "_" + value + "_0"); //get the first page

                    while (curPage != null) { //loop over all overflow pages
                        int prevsize = curPage.size();
                        curPage.deletePointer(temp);
                        if (curPage.size() < prevsize) break;

                        curPage = Utilities.deserializeOverflow(curPage.getNext()); //next page
                    }
                }
            }
		}	
	}

}
