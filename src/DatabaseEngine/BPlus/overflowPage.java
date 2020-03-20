package DatabaseEngine.BPlus;

import DatabaseEngine.Utilities;
import DatabaseEngine.pointer;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

public class overflowPage implements Serializable {
    private ArrayList<pointer> pointers;
    private String name;
    private String next;
    private int ID;
    private String prev;

    public overflowPage(overflowPage prev){ //constructor for non first pages
        this.name = prev.name;
        ID = prev.getID() + 1;
        pointers = new ArrayList<>();
        next = null; //i know that it's already null, it looks nicer that way.
        this.prev = prev.name + prev.getID();
        prev.next = this.name + ID;
    }

    public overflowPage(String name){ //constructor for the first page
        this.name = name;
        ID = 0;
        pointers = new ArrayList<>();
        next = null;
        prev = null;
        prev= null;
    }

    public void insert(pointer p){
        pointers.add(p);
    }

    public void deletePointer(pointer p){
        pointers.remove(p);
    }

    public int size(){
        return pointers.size();
    }



    public void destroy(){
        File file = new File("data//BPlus//overflow_Pages//" + "overflow_" + name + ".class");

        if (prev == null){ //if we are deleting the first page
            if(next != null){ //make the next page the first
                overflowPage nextPage = Utilities.deserializeBOverflow(next);
                nextPage.setPrev(null);
                nextPage.setName(name + ID);
                Utilities.serializeBOverflow(nextPage);
            }
        }
        else { //not the first page (remove myself and adjust pointers)
            overflowPage prevPage = Utilities.deserializeBOverflow(prev);
            if (next != null){
                overflowPage nextPage = Utilities.deserializeBOverflow(next);
                nextPage.prev = prev;
                Utilities.serializeBOverflow(nextPage);
            }
            prevPage.next = next;
            Utilities.serializeBOverflow(prevPage);
        }

        if(!file.delete())
            System.out.println("Failed to delete file!");

    }

    public pointer poll() {
        pointer ret = pointers.remove(0);
        if (this.size() == 0) this.destroy();
        return ret;
    }

    public void removeIndex(int index){
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

    public Queue<pointer> getPointers() {
        Queue<pointer> ret = new LinkedList<>();
        ret.addAll(pointers);
        return ret;
    }
}
