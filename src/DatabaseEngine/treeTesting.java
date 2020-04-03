package DatabaseEngine;

import DatabaseEngine.BPlus.BPTExternal;
import DatabaseEngine.BPlus.BPlusTree;
import DatabaseEngine.BPlus.BPointer;

import java.awt.*;
import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;

public class treeTesting {
    public static void main(String[] args) throws DBAppException, ParseException, InterruptedException {
        //INSERTION: PASSED
        //SEARCH: PASSED
        //CORRECT NODES ON DISK: PASSED
        //POINTER SHIFTING ON INSERTION: PASSED
        //OVERFLOW PAGES: PASSED
//        DBApp a = new DBApp();
//        a.init();
//        tree.insert(5,new BPointer(3,1),false);
//        tree.insert(6,new BPointer(3,1),false);
//        tree.insert(7,new BPointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);
//        tree.insert(val,new pointer(3,1),false);

//        tree.insert(3,new pointer(3,2),true);
//        tree.insert(3,new pointer(3,3),true);
//        tree.insert(3,new pointer(3,4),true);
//        tree.insert(3,new pointer(3,5),true);
//        tree.insert(3,new pointer(3,6),true);
//        tree.insert(3,new pointer(3,7),true);
//        tree.insert(3,new pointer(3,8),true);
//        tree.insert(3,new pointer(3,9),true);
//        tree.insert(3,new pointer(3,10),true);
//        tree.insert(3,new pointer(3,11),true);
//        tree.insert(3,new pointer(3,12),true);
//        tree.insert(2,new pointer(2,1),true);
//        tree.insert(2,new pointer(2,2),true);
//        tree.insert(2,new pointer(2,3),true);
//        tree.insert(2,new pointer(2,4),true);
//        tree.insert(2,new pointer(2,5),true);
//        tree.insert(2,new pointer(2,6),true);
//        tree.insert(2,new pointer(2,7),true);
//        tree.insert(2,new pointer(2,8),true);
//        tree.insert(2,new pointer(2,9),true);
//        tree.insert(2,new pointer(2,10),true);
//        tree.insert(2,new pointer(2,11),true);
//        tree.insert(2,new pointer(2,12),true);
//        tree.insert(1,new pointer(1,1),true);
//        tree.insert(1,new pointer(1,2),true);
//        tree.insert(1,new pointer(1,3),true);
//        tree.insert(1,new pointer(1,4),true);
//        tree.insert(1,new pointer(1,5),true);
//        tree.insert(1,new pointer(1,6),true);
//        tree.insert(1,new pointer(1,7),true);
//        tree.insert(1,new pointer(1,8),true);
//        tree.insert(1,new pointer(1,9),true);
//        tree.insert(1,new pointer(1,10),true);
//        tree.insert(1,new pointer(1,11),true);
//        tree.insert(1,new pointer(1,12),true);
//
//        BSet<pointer> ret = tree.search(2,"<=");
//        Iterator p = ret.iterator();
//        while (p.hasNext()){
//            pointer cur = (pointer) p.next();
//            System.out.println(cur.getPage() + " " + cur.getOffset());
//        }
//
//        ret = tree.search(3,"=");
//        p = ret.iterator();
//        while (p.hasNext()){
//            pointer cur = (pointer) p.next();
//            System.out.println(cur.getPage() + " " + cur.getOffset());
//        }
    }
}
