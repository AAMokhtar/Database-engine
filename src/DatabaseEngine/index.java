package DatabaseEngine;

import DatabaseEngine.BPlus.pointer;

import java.util.ArrayList;

public interface index<T> { // B+, R

    //required methods:
    public void insert(T value, pointer recordPointer) throws DBAppException;
    public void delete(T key);
    public ArrayList<pointer> search(T key);
}
