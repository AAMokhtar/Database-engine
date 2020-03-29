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

    public overflowPage(overflowPage prev){ //constructor for non first pages
        this.name = prev.name;
        ID = prev.getID() + 1;
        pointers = new ArrayList<>();
        next = null; //i know that it's already null, it looks nicer that way.
        this.prev = prev.name +"_"+ prev.getID();
        prev.next = this.name +"_"+ ID;
    }

    public overflowPage(String name){ //constructor for the first page
        this.name = name;
        ID = 0;
        pointers = new ArrayList<>();
        next = null;
        prev = null;
        prev= null;
    }

    public void insert(Pointer p){
        pointers.add(p);
    } //DO NOT USE THIS METHOD TO INSERT

    public void deletePointer(Pointer p){ //deletes a pointer value
        pointers.remove(p);
    }

    public int size(){
        return pointers.size();
    } //page size



    public void destroy(){ //deletes the page and fixes the pointers to the previous and next pages
        File file = new File("data//overflow_Pages//" + "overflow_" + name + ".class");

        if (ID == 0){ //if we are deleting the first page
            if(next != null){ //make the next page the first
                overflowPage nextPage = Utilities.deserializeOverflow(next);
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

        if(!file.delete())
            System.out.println("Failed to delete file!");

    }

    public Pointer poll() { //remove first element and return it
        Pointer ret = pointers.remove(0);
        if (this.size() == 0) this.destroy();
        return ret;
    }

    public void removeIndex(int index){ //remove an pointer from the page using its index in the array
        pointers.remove(index);
        if (this.size() == 0) this.destroy();
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

    public void setID(int ID) {
        this.ID = ID;
    }

    public Queue<Pointer> getPointers() {
        Queue<Pointer> ret = new LinkedList<>();
        ret.addAll(pointers);
        return ret;
    }
}
