package DatabaseEngine;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class overflowPage implements Serializable {
    private ArrayList<Pointer> pointers;
    private String name; //treename_value
    private String next;
    private int ID;
    private String prev;

    private overflowPage(overflowPage prev){ //constructor for non first pages
        this.name = prev.name;
        ID = prev.getID() + 1;
        pointers = new ArrayList<>();
        next = null; //i know that it's already null, it looks nicer that way.
        this.prev = prev.name +"_"+ prev.getID();
        prev.next = this.name +"_"+ ID;
        Utilities.serializeOverflow(this);
        Utilities.serializeOverflow(prev);


    }
public void addPointer(Pointer e) {
        pointers.add(e);
    }


    private overflowPage(String name){ //constructor for the first page
        this.name = name;
        ID = 0;
        pointers = new ArrayList<>();
        next = null;
        prev = null;
        prev= null;
        Utilities.serializeOverflow(this);
    }

    private void insert(Pointer p){
        pointers.add(p);
    } //DO NOT USE THIS METHOD TO INSERT

    public void deletePointer(Pointer p){ //deletes a pointer value
        pointers.remove(p);
        if (this.size() == 0) this.destroy();
        else Utilities.serializeOverflow(this);
    }

    public int size(){
        return pointers.size();
    } //page size



    public void destroy(){ //deletes the page and fixes the pointers to the previous and next pages
        File thisFile = new File("data//overflow_Pages//" + "overflow_" + name+"_"+ID + ".class");

        if(!thisFile.delete()) System.out.println("Failed to delete file!");

        if (ID == 0){ //if we are deleting the first page
            if(next != null){ //make the next page the first
                overflowPage nextPage = Utilities.deserializeOverflow(next);

                File nextFile = new File("data//overflow_Pages//" + "overflow_" + next + ".class");
                if(!nextFile.delete()) System.out.println("Failed to delete file!");

                nextPage.setPrev(null);
                nextPage.setID(0);
                Utilities.serializeOverflow(nextPage);
            }
        }
        else { //not the first page (remove myself and adjust pointers)
            overflowPage prevPage = Utilities.deserializeOverflow(prev);
            if (next != null){
                overflowPage nextPage = Utilities.deserializeOverflow(next);
                nextPage.prev = prev;
                Utilities.serializeOverflow(nextPage);
            }
            prevPage.next = next;
            Utilities.serializeOverflow(prevPage);
        }

    }

    public Pointer poll() { //remove first element and return it
        Pointer ret = pointers.remove(0);

        if (this.size() == 0) this.destroy();
        else Utilities.serializeOverflow(this);

        return ret;
    }

    public void removeIndex(int index){ //remove a pointer from the page using its index in the array
        pointers.remove(index);
        if (this.size() == 0) this.destroy();
        else Utilities.serializeOverflow(this);

    }

    public int getID() {
        return ID;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPrev(String prev) {
        this.prev = prev;
    }

    public String getPrev() {
        return prev;
    }

    public void setNext(String next) {
        this.next = next;
    }

    public String getName() {
        return name;
    }

    public String getNext() {
        return next;
    }

    private void setID(int ID) {
        this.ID = ID;
    }

    public Queue<Pointer> getPointers() {
        Queue<Pointer> ret = new LinkedList<>();
        ret.addAll(pointers);
        return ret;
    }

    //insert a value at the end of the overflow pages
    public static void insert(String treeName_value, Pointer recordPointer){

        //get the maximum number of tuples per page:
        int N = 0;
        try {
            N = Utilities.readNodeSize("config//DBApp.properties");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            System.out.println("insertion failed");
            e.printStackTrace();
            return;
        }

        //the first overflow page does not exist
        String path = "data//overflow_Pages//" + "overflow_" + treeName_value + "_0.class";
        path = path.replaceAll("[^a-zA-Z0-9()_./+]",""); //windows is gay

        if (!new File(path).isFile()){
            overflowPage firstPage = new overflowPage(treeName_value);
            firstPage.insert(recordPointer);
            Utilities.serializeOverflow(firstPage);
        }

        //the first overflow page exists
        else {
            overflowPage curPage = Utilities.deserializeOverflow(treeName_value + "_0"); //get the first page

            while (curPage.getNext() != null){ //a suitable page or the last page
                if (curPage.size() < N){
                    break;
                }

                curPage = Utilities.deserializeOverflow(curPage.getNext());
            }

            if (curPage.size() < N) { //a vacant space exists
                curPage.insert(recordPointer);
            }

            else { //create new page
                overflowPage lastPage = new overflowPage(curPage);
                lastPage.insert(recordPointer);
                Utilities.serializeOverflow(lastPage);
            }
            Utilities.serializeOverflow(curPage);
        }
    }

    //remove all overflow pages for a value
    public static void destroyAllPages(String treeName_value){

        //first page path
        String path = "data//overflow_Pages//" + "overflow_" + treeName_value + "_0.class";
        path = path.replaceAll("[^a-zA-Z0-9()_./+]",""); //windows is gay
        File curFile = new File(path);

        //first page exists
        if (curFile.isFile()){

            overflowPage curPage =  Utilities.deserializeOverflow(treeName_value + "_0");

            while (true) {

                //delete page
                if (!curFile.delete())
                    System.out.println("Failed to delete file: " + treeName_value + "_0");

                //last page
                if (curPage.getNext() == null) break;

                //get next page
                path = "data//overflow_Pages//" + "overflow_" + curPage.getNext() + ".class";
                curFile = new File(path);
                curPage = Utilities.deserializeOverflow(curPage.getNext());
            }
        }

    }
}
