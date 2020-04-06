package DatabaseEngine.testing;

import DatabaseEngine.*;
import DatabaseEngine.BPlus.BPTExternal;
import DatabaseEngine.BPlus.BPTInternal;
import DatabaseEngine.BPlus.BPlusTree;
import DatabaseEngine.BPlus.BPointer;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.awt.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)

//working with the assumption that insert/delete methods of the table are correct
public class treeTest {
    private static DBApp DB;

    @BeforeClass
    public static void prepareTrees(){
        DB = new DBApp(); //initialize the engine

        File dir = new File("config//DBApp.properties");
        dir.delete(); //delete properties file

        dir = new File("data"); //delete pages, tables, and metadata.

        for(File file: dir.listFiles())
            if (!file.isDirectory())
                file.delete();

        dir = new File("data//overflow_Pages"); //delete all overflow pages

        for(File file: dir.listFiles())
            if (!file.isDirectory())
                file.delete();

        dir = new File("data//BPlus//B+_Nodes"); //delete all BPlus tree nodes

        for(File file: dir.listFiles())
            if (!file.isDirectory())
                file.delete();

        dir = new File("data//BPlus//Trees"); //delete BPlus trees

        for(File file: dir.listFiles())
            if (!file.isDirectory())
                file.delete();

    //----------------------------------------------------------------------------
        initializeTestProperties();
        DB.init();

        Hashtable table = new Hashtable<String, String>();
        table.put("Integer","java.lang.Integer");
        table.put("Double","java.lang.Double");
        table.put("String","java.lang.String");
        table.put("Date","java.util.Date");
        table.put("Boolean","java.lang.Boolean");
        table.put("Polygon","java.awt.Polygon");

        //create the testing table
        try {
            DB.createTable("TreeTesting","Integer", table);
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void A_createIntegerTree() throws DBAppException {
        DB.createBTreeIndex("TreeTesting","Integer");

        boolean treeExists = new File("data//BPlus//Trees//BPlusTree_TreeTesting_Integer.class").isFile();
        assertTrue(treeExists);
    }

    //===============================TREE_INSERTION===================================
    @Test
    public void B_testInitialScenarioI() throws DBAppException, ParseException {
        initialScenario();

        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTExternal<Integer>  left = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTExternal<Integer>  right = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> leftVals = left.getValues();
        ArrayList<Integer> rightVals = right.getValues();
        ArrayList<BPointer> leftPointers = left.getPointers();
        ArrayList<BPointer> rightPointers = right.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 1,"incorrect node size");
        assertTrue(rootVals.size() == 1,"incorrect node size");
        assertTrue(rootPointers.size() == 2,"incorrect node size");


        assertTrue(left.getSize() == 3,"incorrect node size");
        assertTrue(leftVals.size() == 3,"incorrect node size");
        assertTrue(leftPointers.size() == 3,"incorrect node size");

        assertTrue(right.getSize() == 2,"incorrect node size");
        assertTrue(rightVals.size() == 2,"incorrect node size");
        assertTrue(rightPointers.size() == 2,"incorrect node size");


        //nodes are linked correctly
        assertTrue(left.getNext().equals(rootPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(right.getNext() == null,"leaves are linked incorrectly");

        //values are correct
        assertTrue(rootVals.get(0) == 16,"incorrect value");
        assertTrue(leftVals.get(0) == 1,"incorrect value");
        assertTrue(leftVals.get(1) == 4,"incorrect value");
        assertTrue(leftVals.get(2) == 9,"incorrect value");
        assertTrue(rightVals.get(0) == 16,"incorrect value");
        assertTrue(rightVals.get(1) == 25,"incorrect value");

        //pointers are correct
        assertTrue(leftPointers.get(0).getPage() == 1 && leftPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(leftPointers.get(1).getPage() == 1 && leftPointers.get(1).getOffset() == 1,"incorrect pointer");
        assertTrue(leftPointers.get(2).getPage() == 1 && leftPointers.get(2).getOffset() == 2,"incorrect pointer");

        assertTrue(rightPointers.get(0).getPage() == 2 && rightPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(rightPointers.get(1).getPage() == 2 && rightPointers.get(1).getOffset() == 1,"incorrect pointer");
        assertTrue(right.getNext() == null);
    }

    @Test
    public void C_testInsert20() throws DBAppException, ParseException {
        insert(20,20.1,"J", "2020-03-20", true, 20);


        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTExternal<Integer>  left = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTExternal<Integer>  right = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> leftVals = left.getValues();
        ArrayList<Integer> rightVals = right.getValues();
        ArrayList<BPointer> leftPointers = left.getPointers();
        ArrayList<BPointer> rightPointers = right.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 1,"incorrect node size");
        assertTrue(rootVals.size() == 1,"incorrect node size");
        assertTrue(rootPointers.size() == 2,"incorrect node size");


        assertTrue(left.getSize() == 3,"incorrect node size");
        assertTrue(leftVals.size() == 3,"incorrect node size");
        assertTrue(leftPointers.size() == 3,"incorrect node size");

        assertTrue(right.getSize() == 3,"incorrect node size");
        assertTrue(rightVals.size() == 3,"incorrect node size");
        assertTrue(rightPointers.size() == 3,"incorrect node size");


        //nodes are linked correctly
        assertTrue(left.getNext().equals(rootPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(right.getNext() == null,"leaves are linked incorrectly");

        //values are correct
        assertTrue(rootVals.get(0) == 16,"incorrect value");
        assertTrue(leftVals.get(0) == 1,"incorrect value");
        assertTrue(leftVals.get(1) == 4,"incorrect value");
        assertTrue(leftVals.get(2) == 9,"incorrect value");
        assertTrue(rightVals.get(0) == 16,"incorrect value");
        assertTrue(rightVals.get(1) == 20,"incorrect value");
        assertTrue(rightVals.get(2) == 25,"incorrect value");


        //pointers are correct
        assertTrue(leftPointers.get(0).getPage() == 1 && leftPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(leftPointers.get(1).getPage() == 1 && leftPointers.get(1).getOffset() == 1,"incorrect pointer");
        assertTrue(leftPointers.get(2).getPage() == 1 && leftPointers.get(2).getOffset() == 2,"incorrect pointer");

        assertTrue(rightPointers.get(0).getPage() == 2 && rightPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(rightPointers.get(1).getPage() == 2 && rightPointers.get(1).getOffset() == 1,"incorrect pointer");
        assertTrue(rightPointers.get(2).getPage() == 2 && rightPointers.get(2).getOffset() == 2,"incorrect pointer");

    }

    @Test
    public void D_testInsert13() throws DBAppException, ParseException {
        insert(13,13.2,"G", "2020-03-13", true, 13);


        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTExternal<Integer>  left = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTExternal<Integer>  mid = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));
        BPTExternal<Integer>  right = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(2));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> leftVals = left.getValues();
        ArrayList<Integer> midVals = mid.getValues();
        ArrayList<Integer> rightVals = right.getValues();
        ArrayList<BPointer> leftPointers = left.getPointers();
        ArrayList<BPointer> midPointers = mid.getPointers();
        ArrayList<BPointer> rightPointers = right.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 2,"incorrect node size");
        assertTrue(rootVals.size() == 2,"incorrect node size");
        assertTrue(rootPointers.size() == 3,"incorrect node size");

        assertTrue(left.getSize() == 2,"incorrect node size");
        assertTrue(leftVals.size() == 2,"incorrect node size");
        assertTrue(leftPointers.size() == 2,"incorrect node size");

        assertTrue(mid.getSize() == 2,"incorrect node size");
        assertTrue(midVals.size() == 2,"incorrect node size");
        assertTrue( midPointers.size() == 2,"incorrect node size");

        assertTrue(right.getSize() == 3,"incorrect node size");
        assertTrue(rightVals.size() == 3,"incorrect node size");
        assertTrue(rightPointers.size() == 3,"incorrect node size");


        //nodes are linked correctly
        assertTrue(left.getNext().equals(rootPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(mid.getNext().equals(rootPointers.get(2)),"leaves are linked incorrectly");
        assertTrue(right.getNext() == null,"leaves are linked incorrectly");


        //values are correct
        assertTrue(rootVals.get(0) == 9,"incorrect value");
        assertTrue(rootVals.get(1) == 16,"incorrect value");

        assertTrue(leftVals.get(0) == 1,"incorrect value");
        assertTrue(leftVals.get(1) == 4,"incorrect value");

        assertTrue(midVals.get(0) == 9,"incorrect value");
        assertTrue(midVals.get(1) == 13,"incorrect value");

        assertTrue(rightVals.get(0) == 16,"incorrect value");
        assertTrue(rightVals.get(1) == 20,"incorrect value");
        assertTrue(rightVals.get(2) == 25,"incorrect value");


        //pointers are correct
        assertTrue(leftPointers.get(0).getPage() == 1 && leftPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(leftPointers.get(1).getPage() == 1 && leftPointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(midPointers.get(0).getPage() == 1 && midPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(midPointers.get(1).getPage() == 2 && midPointers.get(1).getOffset() == 0,"incorrect pointer");

        assertTrue(rightPointers.get(0).getPage() == 2 && rightPointers.get(0).getOffset() == 1,"incorrect pointer");
        assertTrue(rightPointers.get(1).getPage() == 2 && rightPointers.get(1).getOffset() == 2,"incorrect pointer");
        assertTrue(rightPointers.get(2).getPage() == 3 && rightPointers.get(2).getOffset() == 0,"incorrect pointer");

    }

    @Test
    public void E_testInsert15() throws DBAppException, ParseException {
        insert(15,15.7,"H", "2020-03-15", true, 15);


        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTExternal<Integer>  left = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTExternal<Integer>  mid = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));
        BPTExternal<Integer>  right = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(2));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> leftVals = left.getValues();
        ArrayList<Integer> midVals = mid.getValues();
        ArrayList<Integer> rightVals = right.getValues();
        ArrayList<BPointer> leftPointers = left.getPointers();
        ArrayList<BPointer> midPointers = mid.getPointers();
        ArrayList<BPointer> rightPointers = right.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 2,"incorrect node size");
        assertTrue(rootVals.size() == 2,"incorrect node size");
        assertTrue(rootPointers.size() == 3,"incorrect node size");

        assertTrue(left.getSize() == 2,"incorrect node size");
        assertTrue(leftVals.size() == 2,"incorrect node size");
        assertTrue(leftPointers.size() == 2,"incorrect node size");

        assertTrue(mid.getSize() == 3,"incorrect node size");
        assertTrue(midVals.size() == 3,"incorrect node size");
        assertTrue( midPointers.size() == 3,"incorrect node size");

        assertTrue(right.getSize() == 3,"incorrect node size");
        assertTrue(rightVals.size() == 3,"incorrect node size");
        assertTrue(rightPointers.size() == 3,"incorrect node size");


        //nodes are linked correctly
        assertTrue(left.getNext().equals(rootPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(mid.getNext().equals(rootPointers.get(2)),"leaves are linked incorrectly");
        assertTrue(right.getNext() == null,"leaves are linked incorrectly");


        //values are correct
        assertTrue(rootVals.get(0) == 9,"incorrect value");
        assertTrue(rootVals.get(1) == 16,"incorrect value");

        assertTrue(leftVals.get(0) == 1,"incorrect value");
        assertTrue(leftVals.get(1) == 4,"incorrect value");

        assertTrue(midVals.get(0) == 9,"incorrect value");
        assertTrue(midVals.get(1) == 13,"incorrect value");
        assertTrue(midVals.get(2) == 15,"incorrect value");


        assertTrue(rightVals.get(0) == 16,"incorrect value");
        assertTrue(rightVals.get(1) == 20,"incorrect value");
        assertTrue(rightVals.get(2) == 25,"incorrect value");


        //pointers are correct
        assertTrue(leftPointers.get(0).getPage() == 1 && leftPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(leftPointers.get(1).getPage() == 1 && leftPointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(midPointers.get(0).getPage() == 1 && midPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(midPointers.get(1).getPage() == 2 && midPointers.get(1).getOffset() == 0,"incorrect pointer");
        assertTrue(midPointers.get(2).getPage() == 2 && midPointers.get(2).getOffset() == 1,"incorrect pointer");


        assertTrue(rightPointers.get(0).getPage() == 2 && rightPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(rightPointers.get(1).getPage() == 3 && rightPointers.get(1).getOffset() == 0,"incorrect pointer");
        assertTrue(rightPointers.get(2).getPage() == 3 && rightPointers.get(2).getOffset() == 1,"incorrect pointer");
    }

    @Test
    public void F_testInsert10() throws DBAppException, ParseException {
        insert(10,10.56,"D", "2020-03-10", false, 10);


        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTExternal<Integer>  one = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTExternal<Integer>  two = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));
        BPTExternal<Integer>  three = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(2));
        BPTExternal<Integer>  four = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(3));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> oneVals = one.getValues();
        ArrayList<Integer> twoVals = two.getValues();
        ArrayList<Integer> threeVals = three.getValues();
        ArrayList<Integer> fourVals = four.getValues();

        ArrayList<BPointer> onePointers = one.getPointers();
        ArrayList<BPointer> twoPointers = two.getPointers();
        ArrayList<BPointer> threePointers = three.getPointers();
        ArrayList<BPointer> fourPointers = four.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 3,"incorrect node size");
        assertTrue(rootVals.size() == 3,"incorrect node size");
        assertTrue(rootPointers.size() == 4,"incorrect node size");

        assertTrue(one.getSize() == 2,"incorrect node size");
        assertTrue(oneVals.size() == 2,"incorrect node size");
        assertTrue(onePointers.size() == 2,"incorrect node size");

        assertTrue(two.getSize() == 2,"incorrect node size");
        assertTrue(twoVals.size() == 2,"incorrect node size");
        assertTrue( twoPointers.size() == 2,"incorrect node size");

        assertTrue(three.getSize() == 2,"incorrect node size");
        assertTrue(threeVals.size() == 2,"incorrect node size");
        assertTrue(threePointers.size() == 2,"incorrect node size");

        assertTrue(four.getSize() == 3,"incorrect node size");
        assertTrue(fourVals.size() == 3,"incorrect node size");
        assertTrue(fourPointers.size() == 3,"incorrect node size");


        //nodes are linked correctly
        assertTrue(one.getNext().equals(rootPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(two.getNext().equals(rootPointers.get(2)),"leaves are linked incorrectly");
        assertTrue(three.getNext().equals(rootPointers.get(3)),"leaves are linked incorrectly");
        assertTrue(four.getNext() == null);


        //values are correct
        assertTrue(rootVals.get(0) == 9,"incorrect value");
        assertTrue(rootVals.get(1) == 13,"incorrect value");
        assertTrue(rootVals.get(2) == 16,"incorrect value");

        assertTrue(oneVals.get(0) == 1,"incorrect value");
        assertTrue(oneVals.get(1) == 4,"incorrect value");

        assertTrue(twoVals.get(0) == 9,"incorrect value");
        assertTrue(twoVals.get(1) == 10,"incorrect value");

        assertTrue(threeVals.get(0) == 13);
        assertTrue(threeVals.get(1) == 15);

        assertTrue(fourVals.get(0) == 16,"incorrect value");
        assertTrue(fourVals.get(1) == 20,"incorrect value");
        assertTrue(fourVals.get(2) == 25,"incorrect value");


        //pointers are correct
        assertTrue(onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0,"incorrect pointer");

        assertTrue(threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 1,"incorrect pointer");
        assertTrue(threePointers.get(1).getPage() == 2 && threePointers.get(1).getOffset() == 2,"incorrect pointer");

        assertTrue(fourPointers.get(0).getPage() == 3 && fourPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(fourPointers.get(1).getPage() == 3 && fourPointers.get(1).getOffset() == 1,"incorrect pointer");
        assertTrue(fourPointers.get(2).getPage() == 3 && fourPointers.get(2).getOffset() == 2,"incorrect pointer");
    }

    @Test
    public void G_testInsert11() throws DBAppException, ParseException {
        insert(11,11.23,"E", "2020-03-11", false, 11);


        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTExternal<Integer>  one = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTExternal<Integer>  two = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));
        BPTExternal<Integer>  three = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(2));
        BPTExternal<Integer>  four = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(3));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> oneVals = one.getValues();
        ArrayList<Integer> twoVals = two.getValues();
        ArrayList<Integer> threeVals = three.getValues();
        ArrayList<Integer> fourVals = four.getValues();

        ArrayList<BPointer> onePointers = one.getPointers();
        ArrayList<BPointer> twoPointers = two.getPointers();
        ArrayList<BPointer> threePointers = three.getPointers();
        ArrayList<BPointer> fourPointers = four.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 3,"incorrect node size");
        assertTrue(rootVals.size() == 3,"incorrect node size");
        assertTrue(rootPointers.size() == 4,"incorrect node size");

        assertTrue(one.getSize() == 2,"incorrect node size");
        assertTrue(oneVals.size() == 2,"incorrect node size");
        assertTrue(onePointers.size() == 2,"incorrect node size");

        assertTrue(two.getSize() == 3,"incorrect node size");
        assertTrue(twoVals.size() == 3,"incorrect node size");
        assertTrue( twoPointers.size() == 3,"incorrect node size");

        assertTrue(three.getSize() == 2,"incorrect node size");
        assertTrue(threeVals.size() == 2,"incorrect node size");
        assertTrue(threePointers.size() == 2,"incorrect node size");

        assertTrue(four.getSize() == 3,"incorrect node size");
        assertTrue(fourVals.size() == 3,"incorrect node size");
        assertTrue(fourPointers.size() == 3,"incorrect node size");


        //nodes are linked correctly
        assertTrue(one.getNext().equals(rootPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(two.getNext().equals(rootPointers.get(2)),"leaves are linked incorrectly");
        assertTrue(three.getNext().equals(rootPointers.get(3)),"leaves are linked incorrectly");
        assertTrue(four.getNext() == null);


        //values are correct
        assertTrue(rootVals.get(0) == 9,"incorrect value");
        assertTrue(rootVals.get(1) == 13,"incorrect value");
        assertTrue(rootVals.get(2) == 16,"incorrect value");

        assertTrue(oneVals.get(0) == 1,"incorrect value");
        assertTrue(oneVals.get(1) == 4,"incorrect value");

        assertTrue(twoVals.get(0) == 9,"incorrect value");
        assertTrue(twoVals.get(1) == 10,"incorrect value");
        assertTrue(twoVals.get(2) == 11,"incorrect value");

        assertTrue(threeVals.get(0) == 13,"incorrect value");
        assertTrue(threeVals.get(1) == 15,"incorrect value");

        assertTrue(fourVals.get(0) == 16,"incorrect value");
        assertTrue(fourVals.get(1) == 20,"incorrect value");
        assertTrue(fourVals.get(2) == 25,"incorrect value");


        //pointers are correct
        assertTrue(onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0,"incorrect pointer");
        assertTrue(twoPointers.get(2).getPage() == 2 && twoPointers.get(2).getOffset() == 1,"incorrect pointer");

        assertTrue(threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(threePointers.get(1).getPage() == 3 && threePointers.get(1).getOffset() == 0,"incorrect pointer");

        assertTrue(fourPointers.get(0).getPage() == 3 && fourPointers.get(0).getOffset() == 1,"incorrect pointer");
        assertTrue(fourPointers.get(1).getPage() == 3 && fourPointers.get(1).getOffset() == 2,"incorrect pointer");
        assertTrue(fourPointers.get(2).getPage() == 4 && fourPointers.get(2).getOffset() == 0,"incorrect pointer");
    }

    @Test
    public void H_testInsert12() throws DBAppException, ParseException {
        insert(12,12.49,"F", "2020-03-12", true, 12);

        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTInternal<Integer>  left = (BPTInternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTInternal<Integer>  right = (BPTInternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> leftVals = left.getValues();
        ArrayList<Integer> rightVals = right.getValues();

        ArrayList<String> leftPointers = left.getPointers();
        ArrayList<String> rightPointers = right.getPointers();

        BPTExternal<Integer>  one = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(0));
        BPTExternal<Integer>  two = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(1));
        BPTExternal<Integer>  three = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(2));

        BPTExternal<Integer>  four = (BPTExternal<Integer>) Utilities.deserializeNode(rightPointers.get(0));
        BPTExternal<Integer>  five = (BPTExternal<Integer>) Utilities.deserializeNode(rightPointers.get(1));

        ArrayList<Integer> oneVals = one.getValues();
        ArrayList<Integer> twoVals = two.getValues();
        ArrayList<Integer> threeVals = three.getValues();
        ArrayList<Integer> fourVals = four.getValues();
        ArrayList<Integer> fiveVals = five.getValues();

        ArrayList<BPointer> onePointers = one.getPointers();
        ArrayList<BPointer> twoPointers = two.getPointers();
        ArrayList<BPointer> threePointers = three.getPointers();
        ArrayList<BPointer> fourPointers = four.getPointers();
        ArrayList<BPointer> fivePointers = five.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 1,"incorrect node size");
        assertTrue(rootVals.size() == 1,"incorrect node size");
        assertTrue(rootPointers.size() == 2,"incorrect node size");

        assertTrue(left.getSize() == 2,"incorrect node size");
        assertTrue(leftVals.size() == 2,"incorrect node size");
        assertTrue(leftPointers.size() == 3,"incorrect node size");

        assertTrue(right.getSize() == 1,"incorrect node size");
        assertTrue(rightVals.size() == 1,"incorrect node size");
        assertTrue(rightPointers.size() == 2,"incorrect node size");

        assertTrue(one.getSize() == 2,"incorrect node size");
        assertTrue(oneVals.size() == 2,"incorrect node size");
        assertTrue(onePointers.size() == 2,"incorrect node size");

        assertTrue(two.getSize() == 2,"incorrect node size");
        assertTrue(twoVals.size() == 2,"incorrect node size");
        assertTrue( twoPointers.size() == 2,"incorrect node size");

        assertTrue(three.getSize() == 2,"incorrect node size");
        assertTrue(threeVals.size() == 2,"incorrect node size");
        assertTrue(threePointers.size() == 2,"incorrect node size");

        assertTrue(four.getSize() == 2,"incorrect node size");
        assertTrue(fourVals.size() == 2,"incorrect node size");
        assertTrue(fourPointers.size() == 2,"incorrect node size");

        assertTrue(five.getSize() == 3,"incorrect node size");
        assertTrue(fiveVals.size() == 3,"incorrect node size");
        assertTrue(fivePointers.size() == 3,"incorrect node size");


        //nodes are linked correctly
        assertTrue(one.getNext().equals(leftPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(two.getNext().equals(leftPointers.get(2)),"leaves are linked incorrectly");
        assertTrue(three.getNext().equals(rightPointers.get(0)),"leaves are linked incorrectly");
        assertTrue(four.getNext().equals(rightPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(five.getNext() == null);


        //values are correct
        assertTrue(rootVals.get(0) == 13,"incorrect value");

        assertTrue(leftVals.get(0) == 9,"incorrect value");
        assertTrue(leftVals.get(1) == 11,"incorrect value");

        assertTrue(rightVals.get(0) == 16,"incorrect value");

        assertTrue(oneVals.get(0) == 1,"incorrect value");
        assertTrue(oneVals.get(1) == 4,"incorrect value");

        assertTrue(twoVals.get(0) == 9,"incorrect value");
        assertTrue(twoVals.get(1) == 10,"incorrect value");

        assertTrue(threeVals.get(0) == 11,"incorrect value");
        assertTrue(threeVals.get(1) == 12,"incorrect value");

        assertTrue(fourVals.get(0) == 13,"incorrect value");
        assertTrue(fourVals.get(1) == 15,"incorrect value");

        assertTrue(fiveVals.get(0) == 16,"incorrect value");
        assertTrue(fiveVals.get(1) == 20,"incorrect value");
        assertTrue(fiveVals.get(2) == 25,"incorrect value");



        //pointers are correct
        assertTrue(onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0,"incorrect pointer");

        assertTrue(threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 1,"incorrect pointer");
        assertTrue(threePointers.get(1).getPage() == 2 && threePointers.get(1).getOffset() == 2,"incorrect pointer");

        assertTrue(fourPointers.get(0).getPage() == 3 && fourPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(fourPointers.get(1).getPage() == 3 && fourPointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(fivePointers.get(0).getPage() == 3 && fivePointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(fivePointers.get(1).getPage() == 4 && fivePointers.get(1).getOffset() == 0,"incorrect pointer");
        assertTrue(fivePointers.get(2).getPage() == 4 && fivePointers.get(2).getOffset() == 1,"incorrect pointer");
    }

    //================================TREE_OVERFLOW===================================

    @Test
    public void I_overflowInsert() throws DBAppException, ParseException {
        for (int i = 0; i < 9; i++)  //3 full pages
            insert(13,13.2,"G", "2020-03-13", true, 13);

        String path = "data//overflow_Pages//overflow_TreeTesting_Integer_13_0.class";
        assertTrue(new File(path).isFile()); //first page

        overflowPage[] pages = new overflowPage[3];

        pages[0] = Utilities.deserializeOverflow("TreeTesting_Integer_13_0");
        assertEquals("TreeTesting_Integer_13_1", pages[0].getNext(),"overflow pages are not linked correctly");
        assertEquals(null, pages[0].getPrev(),"overflow pages are not linked correctly");

        pages[1] = Utilities.deserializeOverflow("TreeTesting_Integer_13_1");
        assertEquals("TreeTesting_Integer_13_2", pages[1].getNext(),"overflow pages are not linked correctly");
        assertEquals("TreeTesting_Integer_13_0", pages[1].getPrev(),"overflow pages are not linked correctly");

        pages[2] = Utilities.deserializeOverflow("TreeTesting_Integer_13_2");
        assertEquals(null, pages[2].getNext(),"overflow pages are not linked correctly");
        assertEquals("TreeTesting_Integer_13_1", pages[2].getPrev(),"overflow pages are not linked correctly");

        //sizes
        assertEquals(3, pages[0].size(),"page size is incorrect");
        assertEquals(3, pages[1].size(),"page size is incorrect");
        assertEquals(3, pages[2].size(),"page size is incorrect");

        //correct pointers
        for (int i = 0; i < 3; i++) {
            Queue<Pointer> curPage = pages[i].getPointers();
            int j = 0;
            while (!curPage.isEmpty()){
                BPointer curPointer = (BPointer) curPage.poll();

                assertEquals(2 - j, curPointer.getOffset(),"overflow pointer offeset is incorrect");
                assertEquals(5 - i, curPointer.getPage(),"overflow pointer page is incorrect");

                j++;
            }

        }

        //-------------------------tree structure------------------------------

        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTInternal<Integer>  left = (BPTInternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTInternal<Integer>  right = (BPTInternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> leftVals = left.getValues();
        ArrayList<Integer> rightVals = right.getValues();

        ArrayList<String> leftPointers = left.getPointers();
        ArrayList<String> rightPointers = right.getPointers();

        BPTExternal<Integer>  one = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(0));
        BPTExternal<Integer>  two = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(1));
        BPTExternal<Integer>  three = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(2));

        BPTExternal<Integer>  four = (BPTExternal<Integer>) Utilities.deserializeNode(rightPointers.get(0));
        BPTExternal<Integer>  five = (BPTExternal<Integer>) Utilities.deserializeNode(rightPointers.get(1));

        ArrayList<Integer> oneVals = one.getValues();
        ArrayList<Integer> twoVals = two.getValues();
        ArrayList<Integer> threeVals = three.getValues();
        ArrayList<Integer> fourVals = four.getValues();
        ArrayList<Integer> fiveVals = five.getValues();

        ArrayList<BPointer> onePointers = one.getPointers();
        ArrayList<BPointer> twoPointers = two.getPointers();
        ArrayList<BPointer> threePointers = three.getPointers();
        ArrayList<BPointer> fourPointers = four.getPointers();
        ArrayList<BPointer> fivePointers = five.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 1,"incorrect node size");
        assertTrue(rootVals.size() == 1,"incorrect node size");
        assertTrue(rootPointers.size() == 2,"incorrect node size");

        assertTrue(left.getSize() == 2,"incorrect node size");
        assertTrue(leftVals.size() == 2,"incorrect node size");
        assertTrue(leftPointers.size() == 3,"incorrect node size");

        assertTrue(right.getSize() == 1,"incorrect node size");
        assertTrue(rightVals.size() == 1,"incorrect node size");
        assertTrue(rightPointers.size() == 2,"incorrect node size");

        assertTrue(one.getSize() == 2,"incorrect node size");
        assertTrue(oneVals.size() == 2,"incorrect node size");
        assertTrue(onePointers.size() == 2,"incorrect node size");

        assertTrue(two.getSize() == 2,"incorrect node size");
        assertTrue(twoVals.size() == 2,"incorrect node size");
        assertTrue( twoPointers.size() == 2,"incorrect node size");

        assertTrue(three.getSize() == 2,"incorrect node size");
        assertTrue(threeVals.size() == 2,"incorrect node size");
        assertTrue(threePointers.size() == 2,"incorrect node size");

        assertTrue(four.getSize() == 2,"incorrect node size");
        assertTrue(fourVals.size() == 2,"incorrect node size");
        assertTrue(fourPointers.size() == 2,"incorrect node size");

        assertTrue(five.getSize() == 3,"incorrect node size");
        assertTrue(fiveVals.size() == 3,"incorrect node size");
        assertTrue(fivePointers.size() == 3,"incorrect node size");


        //nodes are linked correctly
        assertTrue(one.getNext().equals(leftPointers.get(1)));
        assertTrue(two.getNext().equals(leftPointers.get(2)));
        assertTrue(three.getNext().equals(rightPointers.get(0)));
        assertTrue(four.getNext().equals(rightPointers.get(1)));
        assertTrue(five.getNext() == null);


        //values are correct
        assertTrue(rootVals.get(0) == 13,"incorrect value");

        assertTrue(leftVals.get(0) == 9,"incorrect value");
        assertTrue(leftVals.get(1) == 11,"incorrect value");

        assertTrue(rightVals.get(0) == 16,"incorrect value");

        assertTrue(oneVals.get(0) == 1,"incorrect value");
        assertTrue(oneVals.get(1) == 4,"incorrect value");

        assertTrue(twoVals.get(0) == 9,"incorrect value");
        assertTrue(twoVals.get(1) == 10,"incorrect value");

        assertTrue(threeVals.get(0) == 11,"incorrect value");
        assertTrue(threeVals.get(1) == 12,"incorrect value");

        assertTrue(fourVals.get(0) == 13,"incorrect value");
        assertTrue(fourVals.get(1) == 15,"incorrect value");

        assertTrue(fiveVals.get(0) == 16,"incorrect value");
        assertTrue(fiveVals.get(1) == 20,"incorrect value");
        assertTrue(fiveVals.get(2) == 25,"incorrect value");



        //pointers are correct
        assertTrue(onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0,"incorrect pointer");

        assertTrue(threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 1,"incorrect pointer");
        assertTrue(threePointers.get(1).getPage() == 2 && threePointers.get(1).getOffset() == 2,"incorrect pointer");

        assertTrue(fourPointers.get(0).getPage() == 6 && fourPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(fourPointers.get(1).getPage() == 6 && fourPointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(fivePointers.get(0).getPage() == 6 && fivePointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(fivePointers.get(1).getPage() == 7 && fivePointers.get(1).getOffset() == 0,"incorrect pointer");
        assertTrue(fivePointers.get(2).getPage() == 7 && fivePointers.get(2).getOffset() == 1,"incorrect pointer");

    }

    //===============================TREE_DELETE===================================
    @Test
    public void J_testDelete13() throws DBAppException {
        Hashtable<String, Object> tuple = new Hashtable<>();
        tuple.put("Integer",13);

        DB.deleteFromTable("TreeTesting", tuple); //delete 13 with its 9 duplicates

        File overflow = new File("data//overflow_Pages");
        assertEquals(0,overflow.listFiles().length, "overflow pages still exist after deletion");

        //----------------------------------TREE_STRUCTURE--------------------------------------

        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTInternal<Integer>  left = (BPTInternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTInternal<Integer>  right = (BPTInternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> leftVals = left.getValues();
        ArrayList<Integer> rightVals = right.getValues();

        ArrayList<String> leftPointers = left.getPointers();
        ArrayList<String> rightPointers = right.getPointers();

        BPTExternal<Integer>  one = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(0));
        BPTExternal<Integer>  two = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(1));
        BPTExternal<Integer>  three = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(2));

        BPTExternal<Integer>  four = (BPTExternal<Integer>) Utilities.deserializeNode(rightPointers.get(0));
        BPTExternal<Integer>  five = (BPTExternal<Integer>) Utilities.deserializeNode(rightPointers.get(1));

        ArrayList<Integer> oneVals = one.getValues();
        ArrayList<Integer> twoVals = two.getValues();
        ArrayList<Integer> threeVals = three.getValues();
        ArrayList<Integer> fourVals = four.getValues();
        ArrayList<Integer> fiveVals = five.getValues();

        ArrayList<BPointer> onePointers = one.getPointers();
        ArrayList<BPointer> twoPointers = two.getPointers();
        ArrayList<BPointer> threePointers = three.getPointers();
        ArrayList<BPointer> fourPointers = four.getPointers();
        ArrayList<BPointer> fivePointers = five.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 1,"incorrect node size");
        assertTrue(rootVals.size() == 1,"incorrect node size");
        assertTrue(rootPointers.size() == 2,"incorrect node size");

        assertTrue(left.getSize() == 2,"incorrect node size");
        assertTrue(leftVals.size() == 2,"incorrect node size");
        assertTrue(leftPointers.size() == 3,"incorrect node size");

        assertTrue(right.getSize() == 1,"incorrect node size");
        assertTrue(rightVals.size() == 1,"incorrect node size");
        assertTrue(rightPointers.size() == 2,"incorrect node size");

        assertTrue(one.getSize() == 2,"incorrect node size");
        assertTrue(oneVals.size() == 2,"incorrect node size");
        assertTrue(onePointers.size() == 2,"incorrect node size");

        assertTrue(two.getSize() == 2,"incorrect node size");
        assertTrue(twoVals.size() == 2,"incorrect node size");
        assertTrue( twoPointers.size() == 2,"incorrect node size");

        assertTrue(three.getSize() == 2,"incorrect node size");
        assertTrue(threeVals.size() == 2,"incorrect node size");
        assertTrue(threePointers.size() == 2,"incorrect node size");

        assertTrue(four.getSize() == 1,"incorrect node size");
        assertTrue(fourVals.size() == 1,"incorrect node size");
        assertTrue(fourPointers.size() == 1,"incorrect node size");

        assertTrue(five.getSize() == 3,"incorrect node size");
        assertTrue(fiveVals.size() == 3,"incorrect node size");
        assertTrue(fivePointers.size() == 3,"incorrect node size");


        //nodes are linked correctly
        assertTrue(one.getNext().equals(leftPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(two.getNext().equals(leftPointers.get(2)),"leaves are linked incorrectly");
        assertTrue(three.getNext().equals(rightPointers.get(0)),"leaves are linked incorrectly");
        assertTrue(four.getNext().equals(rightPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(five.getNext() == null,"leaves are linked incorrectly");


        //values are correct
        assertTrue(rootVals.get(0) == 15,"incorrect value");

        assertTrue(leftVals.get(0) == 9,"incorrect value");
        assertTrue(leftVals.get(1) == 11,"incorrect value");

        assertTrue(rightVals.get(0) == 16,"incorrect value");

        assertTrue(oneVals.get(0) == 1,"incorrect value");
        assertTrue(oneVals.get(1) == 4,"incorrect value");

        assertTrue(twoVals.get(0) == 9,"incorrect value");
        assertTrue(twoVals.get(1) == 10,"incorrect value");

        assertTrue(threeVals.get(0) == 11,"incorrect value");
        assertTrue(threeVals.get(1) == 12,"incorrect value");

        assertTrue(fourVals.get(0) == 15,"incorrect value");

        assertTrue(fiveVals.get(0) == 16,"incorrect value");
        assertTrue(fiveVals.get(1) == 20,"incorrect value");
        assertTrue(fiveVals.get(2) == 25,"incorrect value");



        //pointers are correct
        assertTrue(onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0,"incorrect pointer");

        assertTrue(threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 1,"incorrect pointer");
        assertTrue(threePointers.get(1).getPage() == 2 && threePointers.get(1).getOffset() == 2,"incorrect pointer");

        assertTrue(fourPointers.get(0).getPage() == 6 && fourPointers.get(0).getOffset() == 0,"incorrect pointer");

        assertTrue(fivePointers.get(0).getPage() == 6 && fivePointers.get(0).getOffset() == 1,"incorrect pointer");
        assertTrue(fivePointers.get(1).getPage() == 7 && fivePointers.get(1).getOffset() == 0,"incorrect pointer");
        assertTrue(fivePointers.get(2).getPage() == 7 && fivePointers.get(2).getOffset() == 1,"incorrect pointer");

    }

    @Test
    public void K_testDelete15() throws DBAppException {
        Hashtable<String, Object> tuple = new Hashtable<>();
        tuple.put("Integer",15);

        DB.deleteFromTable("TreeTesting", tuple);

        //----------------------------------TREE_STRUCTURE--------------------------------------

        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTInternal<Integer>  left = (BPTInternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTInternal<Integer>  right = (BPTInternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> leftVals = left.getValues();
        ArrayList<Integer> rightVals = right.getValues();

        ArrayList<String> leftPointers = left.getPointers();
        ArrayList<String> rightPointers = right.getPointers();

        BPTExternal<Integer>  one = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(0));
        BPTExternal<Integer>  two = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(1));
        BPTExternal<Integer>  three = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(2));

        BPTExternal<Integer>  four = (BPTExternal<Integer>) Utilities.deserializeNode(rightPointers.get(0));
        BPTExternal<Integer>  five = (BPTExternal<Integer>) Utilities.deserializeNode(rightPointers.get(1));

        ArrayList<Integer> oneVals = one.getValues();
        ArrayList<Integer> twoVals = two.getValues();
        ArrayList<Integer> threeVals = three.getValues();
        ArrayList<Integer> fourVals = four.getValues();
        ArrayList<Integer> fiveVals = five.getValues();

        ArrayList<BPointer> onePointers = one.getPointers();
        ArrayList<BPointer> twoPointers = two.getPointers();
        ArrayList<BPointer> threePointers = three.getPointers();
        ArrayList<BPointer> fourPointers = four.getPointers();
        ArrayList<BPointer> fivePointers = five.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 1,"incorrect node size");
        assertTrue(rootVals.size() == 1,"incorrect node size");
        assertTrue(rootPointers.size() == 2,"incorrect node size");

        assertTrue(left.getSize() == 2,"incorrect node size");
        assertTrue(leftVals.size() == 2,"incorrect node size");
        assertTrue(leftPointers.size() == 3,"incorrect node size");

        assertTrue(right.getSize() == 1,"incorrect node size");
        assertTrue(rightVals.size() == 1,"incorrect node size");
        assertTrue(rightPointers.size() == 2,"incorrect node size");

        assertTrue(one.getSize() == 2,"incorrect node size");
        assertTrue(oneVals.size() == 2,"incorrect node size");
        assertTrue(onePointers.size() == 2,"incorrect node size");

        assertTrue(two.getSize() == 2,"incorrect node size");
        assertTrue(twoVals.size() == 2,"incorrect node size");
        assertTrue( twoPointers.size() == 2,"incorrect node size");

        assertTrue(three.getSize() == 2,"incorrect node size");
        assertTrue(threeVals.size() == 2,"incorrect node size");
        assertTrue(threePointers.size() == 2,"incorrect node size");

        assertTrue(four.getSize() == 1,"incorrect node size");
        assertTrue(fourVals.size() == 1,"incorrect node size");
        assertTrue(fourPointers.size() == 1,"incorrect node size");

        assertTrue(five.getSize() == 2,"incorrect node size");
        assertTrue(fiveVals.size() == 2,"incorrect node size");
        assertTrue(fivePointers.size() == 2,"incorrect node size");


        //nodes are linked correctly
        assertTrue(one.getNext().equals(leftPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(two.getNext().equals(leftPointers.get(2)),"leaves are linked incorrectly");
        assertTrue(three.getNext().equals(rightPointers.get(0)),"leaves are linked incorrectly");
        assertTrue(four.getNext().equals(rightPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(five.getNext() == null,"leaves are linked incorrectly");


        //values are correct
        assertTrue(rootVals.get(0) == 16,"incorrect value");

        assertTrue(leftVals.get(0) == 9,"incorrect value");
        assertTrue(leftVals.get(1) == 11,"incorrect value");

        assertTrue(rightVals.get(0) == 20,"incorrect value");

        assertTrue(oneVals.get(0) == 1,"incorrect value");
        assertTrue(oneVals.get(1) == 4,"incorrect value");

        assertTrue(twoVals.get(0) == 9,"incorrect value");
        assertTrue(twoVals.get(1) == 10,"incorrect value");

        assertTrue(threeVals.get(0) == 11,"incorrect value");
        assertTrue(threeVals.get(1) == 12,"incorrect value");

        assertTrue(fourVals.get(0) == 16,"incorrect value");

        assertTrue(fiveVals.get(0) == 20,"incorrect value");
        assertTrue(fiveVals.get(1) == 25,"incorrect value");



        //pointers are correct
        assertTrue(onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0,"incorrect pointer");

        assertTrue(threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 1,"incorrect pointer");
        assertTrue(threePointers.get(1).getPage() == 2 && threePointers.get(1).getOffset() == 2,"incorrect pointer");

        assertTrue(fourPointers.get(0).getPage() == 6 && fourPointers.get(0).getOffset() == 0,"incorrect pointer");

        assertTrue(fivePointers.get(0).getPage() == 7 && fivePointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(fivePointers.get(1).getPage() == 7 && fivePointers.get(1).getOffset() == 1,"incorrect pointer");
    }

    @Test
    public void L_testDelete16_20() throws DBAppException {
        Hashtable<String, Object> tuple = new Hashtable<>();
        tuple.put("Integer",16);
        DB.deleteFromTable("TreeTesting", tuple);

        tuple.put("Integer",20);
        DB.deleteFromTable("TreeTesting", tuple);

        //----------------------------------TREE_STRUCTURE--------------------------------------

        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTInternal<Integer>  left = (BPTInternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTInternal<Integer>  right = (BPTInternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> leftVals = left.getValues();
        ArrayList<Integer> rightVals = right.getValues();

        ArrayList<String> leftPointers = left.getPointers();
        ArrayList<String> rightPointers = right.getPointers();

        BPTExternal<Integer>  one = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(0));
        BPTExternal<Integer>  two = (BPTExternal<Integer>) Utilities.deserializeNode(leftPointers.get(1));

        BPTExternal<Integer>  three = (BPTExternal<Integer>) Utilities.deserializeNode(rightPointers.get(0));
        BPTExternal<Integer>  four = (BPTExternal<Integer>) Utilities.deserializeNode(rightPointers.get(1));

        ArrayList<Integer> oneVals = one.getValues();
        ArrayList<Integer> twoVals = two.getValues();
        ArrayList<Integer> threeVals = three.getValues();
        ArrayList<Integer> fourVals = four.getValues();

        ArrayList<BPointer> onePointers = one.getPointers();
        ArrayList<BPointer> twoPointers = two.getPointers();
        ArrayList<BPointer> threePointers = three.getPointers();
        ArrayList<BPointer> fourPointers = four.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 1,"incorrect node size");
        assertTrue(rootVals.size() == 1,"incorrect node size");
        assertTrue(rootPointers.size() == 2,"incorrect node size");

        assertTrue(left.getSize() == 1,"incorrect node size");
        assertTrue(leftVals.size() == 1,"incorrect node size");
        assertTrue(leftPointers.size() == 2,"incorrect node size");

        assertTrue(right.getSize() == 1,"incorrect node size");
        assertTrue(rightVals.size() == 1,"incorrect node size");
        assertTrue(rightPointers.size() == 2,"incorrect node size");

        assertTrue(one.getSize() == 2,"incorrect node size");
        assertTrue(oneVals.size() == 2,"incorrect node size");
        assertTrue(onePointers.size() == 2,"incorrect node size");

        assertTrue(two.getSize() == 2,"incorrect node size");
        assertTrue(twoVals.size() == 2,"incorrect node size");
        assertTrue( twoPointers.size() == 2,"incorrect node size");

        assertTrue(three.getSize() == 2,"incorrect node size");
        assertTrue(threeVals.size() == 2,"incorrect node size");
        assertTrue(threePointers.size() == 2,"incorrect node size");

        assertTrue(four.getSize() == 1,"incorrect node size");
        assertTrue(fourVals.size() == 1,"incorrect node size");
        assertTrue(fourPointers.size() == 1,"incorrect node size");

        //nodes are linked correctly
        assertTrue(one.getNext().equals(leftPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(two.getNext().equals(rightPointers.get(0)),"leaves are linked incorrectly");
        assertTrue(three.getNext().equals(rightPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(four.getNext() == null);


        //values are correct
        assertTrue(rootVals.get(0) == 11,"incorrect value");

        assertTrue(leftVals.get(0) == 9,"incorrect value");

        assertTrue(rightVals.get(0) == 25,"incorrect value");

        assertTrue(oneVals.get(0) == 1,"incorrect value");
        assertTrue(oneVals.get(1) == 4,"incorrect value");

        assertTrue(twoVals.get(0) == 9,"incorrect value");
        assertTrue(twoVals.get(1) == 10,"incorrect value");

        assertTrue(threeVals.get(0) == 11,"incorrect value");
        assertTrue(threeVals.get(1) == 12,"incorrect value");

        assertTrue(fourVals.get(0) == 25,"incorrect value");




        //pointers are correct
        assertTrue(onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0,"incorrect pointer");

        assertTrue(threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 1,"incorrect pointer");
        assertTrue(threePointers.get(1).getPage() == 2 && threePointers.get(1).getOffset() == 2,"incorrect pointer");

        assertTrue(fourPointers.get(0).getPage() == 7 && fourPointers.get(0).getOffset() == 0,"incorrect pointer");
    }

    @Test
    public void M_testDelete11_12() throws DBAppException {
        Hashtable<String, Object> tuple = new Hashtable<>();
        tuple.put("Integer",11);
        DB.deleteFromTable("TreeTesting", tuple);

        tuple.put("Integer",12);
        DB.deleteFromTable("TreeTesting", tuple);

        //----------------------------------TREE_STRUCTURE--------------------------------------

        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTExternal<Integer>  left = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTExternal<Integer>  mid = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));
        BPTExternal<Integer>  right = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(2));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> leftVals = left.getValues();
        ArrayList<Integer> midVals = mid.getValues();
        ArrayList<Integer> rightVals = right.getValues();
        ArrayList<BPointer> leftPointers = left.getPointers();
        ArrayList<BPointer> midPointers = mid.getPointers();
        ArrayList<BPointer> rightPointers = right.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 2,"incorrect node size");
        assertTrue(rootVals.size() == 2,"incorrect node size");
        assertTrue(rootPointers.size() == 3,"incorrect node size");

        assertTrue(left.getSize() == 2,"incorrect node size");
        assertTrue(leftVals.size() == 2,"incorrect node size");
        assertTrue(leftPointers.size() == 2,"incorrect node size");

        assertTrue(mid.getSize() == 2,"incorrect node size");
        assertTrue(midVals.size() == 2,"incorrect node size");
        assertTrue( midPointers.size() == 2,"incorrect node size");

        assertTrue(right.getSize() == 1,"incorrect node size");
        assertTrue(rightVals.size() == 1,"incorrect node size");
        assertTrue(rightPointers.size() == 1,"incorrect node size");


        //nodes are linked correctly
        assertTrue(left.getNext().equals(rootPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(mid.getNext().equals(rootPointers.get(2)),"leaves are linked incorrectly");
        assertTrue(right.getNext() == null);


        //values are correct
        assertTrue(rootVals.get(0) == 9,"incorrect value");
        assertTrue(rootVals.get(1) == 25,"incorrect value");

        assertTrue(leftVals.get(0) == 1,"incorrect value");
        assertTrue(leftVals.get(1) == 4,"incorrect value");

        assertTrue(midVals.get(0) == 9,"incorrect value");
        assertTrue(midVals.get(1) == 10,"incorrect value");



        assertTrue(rightVals.get(0) == 25,"incorrect value");


        //pointers are correct
        assertTrue(leftPointers.get(0).getPage() == 1 && leftPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(leftPointers.get(1).getPage() == 1 && leftPointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(midPointers.get(0).getPage() == 1 && midPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(midPointers.get(1).getPage() == 2 && midPointers.get(1).getOffset() == 0,"incorrect pointer");

        assertTrue(rightPointers.get(0).getPage() == 7 && rightPointers.get(0).getOffset() == 0,"incorrect pointer");

    }

    @Test
    public void N_testDelete10_25_9() throws DBAppException {
        Hashtable<String, Object> tuple = new Hashtable<>();
        tuple.put("Integer",10);
        DB.deleteFromTable("TreeTesting", tuple);

        tuple.put("Integer",25);
        DB.deleteFromTable("TreeTesting", tuple);

        tuple.put("Integer",9);
        DB.deleteFromTable("TreeTesting", tuple);

        //----------------------------------TREE_STRUCTURE--------------------------------------

        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTInternal<Integer> root = (BPTInternal<Integer>) treeInt.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTExternal<Integer>  left = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(0));
        BPTExternal<Integer>  right = (BPTExternal<Integer>) Utilities.deserializeNode(rootPointers.get(1));

        ArrayList<Integer> rootVals = root.getValues();
        ArrayList<Integer> leftVals = left.getValues();
        ArrayList<Integer> rightVals = right.getValues();
        ArrayList<BPointer> leftPointers = left.getPointers();
        ArrayList<BPointer> rightPointers = right.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 1,"incorrect node size");
        assertTrue(rootVals.size() == 1,"incorrect node size");
        assertTrue(rootPointers.size() == 2,"incorrect node size");

        assertTrue(left.getSize() == 1,"incorrect node size");
        assertTrue(leftVals.size() == 1,"incorrect node size");
        assertTrue(leftPointers.size() == 1,"incorrect node size");


        assertTrue(right.getSize() == 1,"incorrect node size");
        assertTrue(rightVals.size() == 1,"incorrect node size");
        assertTrue(rightPointers.size() == 1,"incorrect node size");


        //nodes are linked correctly
        assertTrue(left.getNext().equals(rootPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(right.getNext() == null,"leaves are linked incorrectly");


        //values are correct
        assertTrue(rootVals.get(0) == 4,"incorrect value");

        assertTrue(leftVals.get(0) == 1,"incorrect value");

        assertTrue(rightVals.get(0) == 4,"incorrect value");


        //pointers are correct
        assertTrue(leftPointers.get(0).getPage() == 1 && leftPointers.get(0).getOffset() == 0,"incorrect pointer");

        assertTrue(rightPointers.get(0).getPage() == 1 && rightPointers.get(0).getOffset() == 1,"incorrect pointer");

    }

    @Test
    public void O_testDelete1() throws DBAppException {
        Hashtable<String, Object> tuple = new Hashtable<>();
        tuple.put("Integer",1);
        DB.deleteFromTable("TreeTesting", tuple);

        //----------------------------------TREE_STRUCTURE--------------------------------------

        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        BPTExternal<Integer> root = (BPTExternal<Integer>) treeInt.getRoot();
        ArrayList<BPointer> rootPointers = root.getPointers();
        ArrayList<Integer> rootVals = root.getValues();

        //correct sizes
        assertTrue(root.getSize() == 1,"incorrect node size");
        assertTrue(rootVals.size() == 1,"incorrect node size");
        assertTrue(rootPointers.size() == 1,"incorrect node size");

        //nodes are linked correctly
        assertTrue(root.getNext() == null,"leaves are linked incorrectly");


        //values are correct
        assertTrue(rootVals.get(0) == 4,"incorrect value");


        //pointers are correct
        assertTrue(rootPointers.get(0).getPage() == 1 && rootPointers.get(0).getOffset() == 0,"incorrect pointer");

    }

    @Test
    public void P_testEmptyTree() throws DBAppException {
        Hashtable<String, Object> tuple = new Hashtable<>();
        tuple.put("Integer",4);
        DB.deleteFromTable("TreeTesting", tuple);

        //----------------------------------TREE_STRUCTURE--------------------------------------

        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        assertEquals(null,treeInt.getRoot(),"tree root should be null");
    }

    @Test
    public void Q_createAllIndices() throws DBAppException, ParseException {
        insertAll();

        DB.createBTreeIndex("TreeTesting","Double");
        DB.createBTreeIndex("TreeTesting","String");
        DB.createBTreeIndex("TreeTesting","Date");
        DB.createBTreeIndex("TreeTesting","Boolean");
//        DB.createRTreeIndex("TreeTesting","Polygon");

        //trees exist
        assertTrue(new File("data//BPlus//Trees//BPlusTree_TreeTesting_Integer.class").isFile());
        assertTrue(new File("data//BPlus//Trees//BPlusTree_TreeTesting_Double.class").isFile());
        assertTrue(new File("data//BPlus//Trees//BPlusTree_TreeTesting_String.class").isFile());
        assertTrue(new File("data//BPlus//Trees//BPlusTree_TreeTesting_Date.class").isFile());
        assertTrue(new File("data//BPlus//Trees//BPlusTree_TreeTesting_Boolean.class").isFile());
//        assertTrue(new File(Polygon).isFile());
    }

    @Test
    public void R_testDouble(){
        BPlusTree<Double> treeDouble = Utilities.deserializeBPT("TreeTesting_Double");
        BPTInternal<Double> root = (BPTInternal<Double>) treeDouble.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTInternal<Double>  left = (BPTInternal<Double>) Utilities.deserializeNode(rootPointers.get(0));
        BPTInternal<Double>  right = (BPTInternal<Double>) Utilities.deserializeNode(rootPointers.get(1));

        ArrayList<Double> rootVals = root.getValues();
        ArrayList<Double> leftVals = left.getValues();
        ArrayList<Double> rightVals = right.getValues();

        ArrayList<String> leftPointers = left.getPointers();
        ArrayList<String> rightPointers = right.getPointers();

        BPTExternal<Double>  one = (BPTExternal<Double>) Utilities.deserializeNode(leftPointers.get(0));
        BPTExternal<Double>  two = (BPTExternal<Double>) Utilities.deserializeNode(leftPointers.get(1));
        BPTExternal<Double>  three = (BPTExternal<Double>) Utilities.deserializeNode(leftPointers.get(2));

        BPTExternal<Double>  four = (BPTExternal<Double>) Utilities.deserializeNode(rightPointers.get(0));
        BPTExternal<Double>  five = (BPTExternal<Double>) Utilities.deserializeNode(rightPointers.get(1));

        ArrayList<Double> oneVals = one.getValues();
        ArrayList<Double> twoVals = two.getValues();
        ArrayList<Double> threeVals = three.getValues();
        ArrayList<Double> fourVals = four.getValues();
        ArrayList<Double> fiveVals = five.getValues();

        ArrayList<BPointer> onePointers = one.getPointers();
        ArrayList<BPointer> twoPointers = two.getPointers();
        ArrayList<BPointer> threePointers = three.getPointers();
        ArrayList<BPointer> fourPointers = four.getPointers();
        ArrayList<BPointer> fivePointers = five.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 1,"incorrect node size");
        assertTrue(rootVals.size() == 1,"incorrect node size");
        assertTrue(rootPointers.size() == 2,"incorrect node size");

        assertTrue(left.getSize() == 2,"incorrect node size");
        assertTrue(leftVals.size() == 2,"incorrect node size");
        assertTrue(leftPointers.size() == 3,"incorrect node size");

        assertTrue(right.getSize() == 1,"incorrect node size");
        assertTrue(rightVals.size() == 1,"incorrect node size");
        assertTrue(rightPointers.size() == 2,"incorrect node size");

        assertTrue(one.getSize() == 2,"incorrect node size");
        assertTrue(oneVals.size() == 2,"incorrect node size");
        assertTrue(onePointers.size() == 2,"incorrect node size");

        assertTrue(two.getSize() == 2,"incorrect node size");
        assertTrue(twoVals.size() == 2,"incorrect node size");
        assertTrue( twoPointers.size() == 2,"incorrect node size");

        assertTrue(three.getSize() == 2,"incorrect node size");
        assertTrue(threeVals.size() == 2,"incorrect node size");
        assertTrue(threePointers.size() == 2,"incorrect node size");

        assertTrue(four.getSize() == 2,"incorrect node size");
        assertTrue(fourVals.size() == 2,"incorrect node size");
        assertTrue(fourPointers.size() == 2,"incorrect node size");

        assertTrue(five.getSize() == 3,"incorrect node size");
        assertTrue(fiveVals.size() == 3,"incorrect node size");
        assertTrue(fivePointers.size() == 3,"incorrect node size");


        //nodes are linked correctly
        assertTrue(one.getNext().equals(leftPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(two.getNext().equals(leftPointers.get(2)),"leaves are linked incorrectly");
        assertTrue(three.getNext().equals(rightPointers.get(0)),"leaves are linked incorrectly");
        assertTrue(four.getNext().equals(rightPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(five.getNext() == null);


        //values are correct
        assertTrue(rootVals.get(0) == 13.2,"incorrect value");

        assertTrue(leftVals.get(0) == 9.8,"incorrect value");
        assertTrue(leftVals.get(1) == 11.23,"incorrect value");

        assertTrue(rightVals.get(0) == 16.3,"incorrect value");

        assertTrue(oneVals.get(0) == 0.9,"incorrect value");
        assertTrue(oneVals.get(1) == 4.6,"incorrect value");

        assertTrue(twoVals.get(0) == 9.8,"incorrect value");
        assertTrue(twoVals.get(1) == 10.56,"incorrect value");

        assertTrue(threeVals.get(0) == 11.23,"incorrect value");
        assertTrue(threeVals.get(1) == 12.49,"incorrect value");

        assertTrue(fourVals.get(0) == 13.2,"incorrect value");
        assertTrue(fourVals.get(1) == 15.7,"incorrect value");

        assertTrue(fiveVals.get(0) == 16.3,"incorrect value");
        assertTrue(fiveVals.get(1) == 20.1,"incorrect value");
        assertTrue(fiveVals.get(2) == 25.9,"incorrect value");



        //pointers are correct
        assertTrue(onePointers.get(0).getPage() == 8 && onePointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(onePointers.get(1).getPage() == 8 && onePointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(twoPointers.get(0).getPage() == 8 && twoPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(twoPointers.get(1).getPage() == 9 && twoPointers.get(1).getOffset() == 0,"incorrect pointer");

        assertTrue(threePointers.get(0).getPage() == 9 && threePointers.get(0).getOffset() == 1,"incorrect pointer");
        assertTrue(threePointers.get(1).getPage() == 9 && threePointers.get(1).getOffset() == 2,"incorrect pointer");

        assertTrue(fourPointers.get(0).getPage() == 10 && fourPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(fourPointers.get(1).getPage() == 10 && fourPointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(fivePointers.get(0).getPage() == 10 && fivePointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(fivePointers.get(1).getPage() == 11 && fivePointers.get(1).getOffset() == 0,"incorrect pointer");
        assertTrue(fivePointers.get(2).getPage() == 11 && fivePointers.get(2).getOffset() == 1,"incorrect pointer");
    }

    @Test
    public void S_testString(){
        BPlusTree<String> treeString = Utilities.deserializeBPT("TreeTesting_String");
        BPTInternal<String> root = (BPTInternal<String>) treeString.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTInternal<String>  left = (BPTInternal<String>) Utilities.deserializeNode(rootPointers.get(0));
        BPTInternal<String>  right = (BPTInternal<String>) Utilities.deserializeNode(rootPointers.get(1));

        ArrayList<String> rootVals = root.getValues();
        ArrayList<String> leftVals = left.getValues();
        ArrayList<String> rightVals = right.getValues();

        ArrayList<String> leftPointers = left.getPointers();
        ArrayList<String> rightPointers = right.getPointers();

        BPTExternal<String>  one = (BPTExternal<String>) Utilities.deserializeNode(leftPointers.get(0));
        BPTExternal<String>  two = (BPTExternal<String>) Utilities.deserializeNode(leftPointers.get(1));
        BPTExternal<String>  three = (BPTExternal<String>) Utilities.deserializeNode(leftPointers.get(2));

        BPTExternal<String>  four = (BPTExternal<String>) Utilities.deserializeNode(rightPointers.get(0));
        BPTExternal<String>  five = (BPTExternal<String>) Utilities.deserializeNode(rightPointers.get(1));

        ArrayList<String> oneVals = one.getValues();
        ArrayList<String> twoVals = two.getValues();
        ArrayList<String> threeVals = three.getValues();
        ArrayList<String> fourVals = four.getValues();
        ArrayList<String> fiveVals = five.getValues();

        ArrayList<BPointer> onePointers = one.getPointers();
        ArrayList<BPointer> twoPointers = two.getPointers();
        ArrayList<BPointer> threePointers = three.getPointers();
        ArrayList<BPointer> fourPointers = four.getPointers();
        ArrayList<BPointer> fivePointers = five.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 1,"incorrect node size");
        assertTrue(rootVals.size() == 1,"incorrect node size");
        assertTrue(rootPointers.size() == 2,"incorrect node size");

        assertTrue(left.getSize() == 2,"incorrect node size");
        assertTrue(leftVals.size() == 2,"incorrect node size");
        assertTrue(leftPointers.size() == 3,"incorrect node size");

        assertTrue(right.getSize() == 1,"incorrect node size");
        assertTrue(rightVals.size() == 1,"incorrect node size");
        assertTrue(rightPointers.size() == 2,"incorrect node size");

        assertTrue(one.getSize() == 2,"incorrect node size");
        assertTrue(oneVals.size() == 2,"incorrect node size");
        assertTrue(onePointers.size() == 2,"incorrect node size");

        assertTrue(two.getSize() == 2,"incorrect node size");
        assertTrue(twoVals.size() == 2,"incorrect node size");
        assertTrue( twoPointers.size() == 2,"incorrect node size");

        assertTrue(three.getSize() == 2,"incorrect node size");
        assertTrue(threeVals.size() == 2,"incorrect node size");
        assertTrue(threePointers.size() == 2,"incorrect node size");

        assertTrue(four.getSize() == 2,"incorrect node size");
        assertTrue(fourVals.size() == 2,"incorrect node size");
        assertTrue(fourPointers.size() == 2,"incorrect node size");

        assertTrue(five.getSize() == 3,"incorrect node size");
        assertTrue(fiveVals.size() == 3,"incorrect node size");
        assertTrue(fivePointers.size() == 3,"incorrect node size");


        //nodes are linked correctly
        assertTrue(one.getNext().equals(leftPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(two.getNext().equals(leftPointers.get(2)),"leaves are linked incorrectly");
        assertTrue(three.getNext().equals(rightPointers.get(0)),"leaves are linked incorrectly");
        assertTrue(four.getNext().equals(rightPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(five.getNext() == null);


        //values are correct
        assertEquals("G",rootVals.get(0),"incorrect value");

        assertEquals("C", leftVals.get(0),"incorrect value");
        assertEquals("E",leftVals.get(1),"incorrect value");

        assertEquals("I", rightVals.get(0),"incorrect value");

        assertEquals("A",oneVals.get(0),"incorrect value");
        assertEquals("B", oneVals.get(1),"incorrect value");

        assertEquals("C",twoVals.get(0),"incorrect value");
        assertEquals("D", twoVals.get(1),"incorrect value");

        assertEquals("E", threeVals.get(0),"incorrect value");
        assertEquals("F", threeVals.get(1),"incorrect value");

        assertEquals("G", fourVals.get(0),"incorrect value");
        assertEquals("H", fourVals.get(1),"incorrect value");

        assertEquals("I", fiveVals.get(0),"incorrect value");
        assertEquals("J", fiveVals.get(1),"incorrect value");
        assertEquals("K", fiveVals.get(2),"incorrect value");



        //pointers are correct
        assertTrue(onePointers.get(0).getPage() == 8 && onePointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(onePointers.get(1).getPage() == 8 && onePointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(twoPointers.get(0).getPage() == 8 && twoPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(twoPointers.get(1).getPage() == 9 && twoPointers.get(1).getOffset() == 0,"incorrect pointer");

        assertTrue(threePointers.get(0).getPage() == 9 && threePointers.get(0).getOffset() == 1,"incorrect pointer");
        assertTrue(threePointers.get(1).getPage() == 9 && threePointers.get(1).getOffset() == 2,"incorrect pointer");

        assertTrue(fourPointers.get(0).getPage() == 10 && fourPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(fourPointers.get(1).getPage() == 10 && fourPointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(fivePointers.get(0).getPage() == 10 && fivePointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(fivePointers.get(1).getPage() == 11 && fivePointers.get(1).getOffset() == 0,"incorrect pointer");
        assertTrue(fivePointers.get(2).getPage() == 11 && fivePointers.get(2).getOffset() == 1,"incorrect pointer");
    }

    @Test
    public void T_testDate() throws ParseException {
        BPlusTree<Date> treeDouble = Utilities.deserializeBPT("TreeTesting_Date");
        BPTInternal<Date> root = (BPTInternal<Date>) treeDouble.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        BPTInternal<Date>  left = (BPTInternal<Date>) Utilities.deserializeNode(rootPointers.get(0));
        BPTInternal<Date>  right = (BPTInternal<Date>) Utilities.deserializeNode(rootPointers.get(1));

        ArrayList<Date> rootVals = root.getValues();
        ArrayList<Date> leftVals = left.getValues();
        ArrayList<Date> rightVals = right.getValues();

        ArrayList<String> leftPointers = left.getPointers();
        ArrayList<String> rightPointers = right.getPointers();

        BPTExternal<Date>  one = (BPTExternal<Date>) Utilities.deserializeNode(leftPointers.get(0));
        BPTExternal<Date>  two = (BPTExternal<Date>) Utilities.deserializeNode(leftPointers.get(1));
        BPTExternal<Date>  three = (BPTExternal<Date>) Utilities.deserializeNode(leftPointers.get(2));

        BPTExternal<Date>  four = (BPTExternal<Date>) Utilities.deserializeNode(rightPointers.get(0));
        BPTExternal<Date>  five = (BPTExternal<Date>) Utilities.deserializeNode(rightPointers.get(1));

        ArrayList<Date> oneVals = one.getValues();
        ArrayList<Date> twoVals = two.getValues();
        ArrayList<Date> threeVals = three.getValues();
        ArrayList<Date> fourVals = four.getValues();
        ArrayList<Date> fiveVals = five.getValues();

        ArrayList<BPointer> onePointers = one.getPointers();
        ArrayList<BPointer> twoPointers = two.getPointers();
        ArrayList<BPointer> threePointers = three.getPointers();
        ArrayList<BPointer> fourPointers = four.getPointers();
        ArrayList<BPointer> fivePointers = five.getPointers();


        //correct sizes
        assertTrue(root.getSize() == 1,"incorrect node size");
        assertTrue(rootVals.size() == 1,"incorrect node size");
        assertTrue(rootPointers.size() == 2,"incorrect node size");

        assertTrue(left.getSize() == 2,"incorrect node size");
        assertTrue(leftVals.size() == 2,"incorrect node size");
        assertTrue(leftPointers.size() == 3,"incorrect node size");

        assertTrue(right.getSize() == 1,"incorrect node size");
        assertTrue(rightVals.size() == 1,"incorrect node size");
        assertTrue(rightPointers.size() == 2,"incorrect node size");

        assertTrue(one.getSize() == 2,"incorrect node size");
        assertTrue(oneVals.size() == 2,"incorrect node size");
        assertTrue(onePointers.size() == 2,"incorrect node size");

        assertTrue(two.getSize() == 2,"incorrect node size");
        assertTrue(twoVals.size() == 2,"incorrect node size");
        assertTrue( twoPointers.size() == 2,"incorrect node size");

        assertTrue(three.getSize() == 2,"incorrect node size");
        assertTrue(threeVals.size() == 2,"incorrect node size");
        assertTrue(threePointers.size() == 2,"incorrect node size");

        assertTrue(four.getSize() == 2,"incorrect node size");
        assertTrue(fourVals.size() == 2,"incorrect node size");
        assertTrue(fourPointers.size() == 2,"incorrect node size");

        assertTrue(five.getSize() == 3,"incorrect node size");
        assertTrue(fiveVals.size() == 3,"incorrect node size");
        assertTrue(fivePointers.size() == 3,"incorrect node size");


        //nodes are linked correctly
        assertTrue(one.getNext().equals(leftPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(two.getNext().equals(leftPointers.get(2)),"leaves are linked incorrectly");
        assertTrue(three.getNext().equals(rightPointers.get(0)),"leaves are linked incorrectly");
        assertTrue(four.getNext().equals(rightPointers.get(1)),"leaves are linked incorrectly");
        assertTrue(five.getNext() == null);


        //values are correct
        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-13"),rootVals.get(0),"incorrect value");

        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-09"), leftVals.get(0),"incorrect value");
        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-11"),leftVals.get(1),"incorrect value");

        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-16"), rightVals.get(0),"incorrect value");

        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-01"),oneVals.get(0),"incorrect value");
        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-04"), oneVals.get(1),"incorrect value");

        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-09"),twoVals.get(0),"incorrect value");
        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-10"), twoVals.get(1),"incorrect value");

        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-11"), threeVals.get(0),"incorrect value");
        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-12"), threeVals.get(1),"incorrect value");

        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-13"), fourVals.get(0),"incorrect value");
        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-15"), fourVals.get(1),"incorrect value");

        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-16"), fiveVals.get(0),"incorrect value");
        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-20"), fiveVals.get(1),"incorrect value");
        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-25"), fiveVals.get(2),"incorrect value");



        //pointers are correct
        assertTrue(onePointers.get(0).getPage() == 8 && onePointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(onePointers.get(1).getPage() == 8 && onePointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(twoPointers.get(0).getPage() == 8 && twoPointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(twoPointers.get(1).getPage() == 9 && twoPointers.get(1).getOffset() == 0,"incorrect pointer");

        assertTrue(threePointers.get(0).getPage() == 9 && threePointers.get(0).getOffset() == 1,"incorrect pointer");
        assertTrue(threePointers.get(1).getPage() == 9 && threePointers.get(1).getOffset() == 2,"incorrect pointer");

        assertTrue(fourPointers.get(0).getPage() == 10 && fourPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(fourPointers.get(1).getPage() == 10 && fourPointers.get(1).getOffset() == 1,"incorrect pointer");

        assertTrue(fivePointers.get(0).getPage() == 10 && fivePointers.get(0).getOffset() == 2,"incorrect pointer");
        assertTrue(fivePointers.get(1).getPage() == 11 && fivePointers.get(1).getOffset() == 0,"incorrect pointer");
        assertTrue(fivePointers.get(2).getPage() == 11 && fivePointers.get(2).getOffset() == 1,"incorrect pointer");
    }

    @Test
    public void U_testBoolean(){
        BPlusTree<Boolean> treeBoolean = Utilities.deserializeBPT("TreeTesting_Boolean");

        BPTExternal<Boolean> root = (BPTExternal<Boolean>) treeBoolean.getRoot();
        ArrayList<Boolean> rootVals = root.getValues();
        ArrayList<BPointer> rootPointers = root.getPointers();

        //correct sizes
        assertTrue(root.getSize() == 2,"incorrect node size");
        assertTrue(rootVals.size() == 2,"incorrect node size");
        assertTrue(rootPointers.size() == 2,"incorrect node size");

        //nodes are linked correctly
        assertTrue(root.getNext() == null);

        //correct values
        assertEquals(false,rootVals.get(0),"incorrect value");
        assertEquals(true,rootVals.get(1),"incorrect value");

        //pointers are correct
        assertTrue(rootPointers.get(0).getPage() == 8 && rootPointers.get(0).getOffset() == 0,"incorrect pointer");
        assertTrue(rootPointers.get(1).getPage() == 9 && rootPointers.get(1).getOffset() == 2,"incorrect pointer");

        String path = "data//overflow_Pages//overflow_TreeTesting_Boolean_false_0.class";
        assertTrue(new File(path).isFile()); //first page

        overflowPage[] falsePages = new overflowPage[2];

        falsePages[0] = Utilities.deserializeOverflow("TreeTesting_Boolean_false_0");
        assertEquals("TreeTesting_Boolean_false_1", falsePages[0].getNext(),"overflow pages are not linked correctly");
        assertEquals(null, falsePages[0].getPrev(),"overflow pages are not linked correctly");

        falsePages[1] = Utilities.deserializeOverflow("TreeTesting_Boolean_false_1");
        assertEquals(null, falsePages[1].getNext(),"overflow pages are not linked correctly");
        assertEquals("TreeTesting_Boolean_false_0", falsePages[1].getPrev(),"overflow pages are not linked correctly");

        //sizes are correct
        assertEquals(3,falsePages[0].size(),"incorrect overflow page size");
        assertEquals(1,falsePages[1].size(),"incorrect overflow page size");

        //pointers are correct
        Queue<Pointer> p =  falsePages[0].getPointers();
        assertTrue(((BPointer) p.peek()).getPage() == 8 && ((BPointer) p.poll()).getOffset() == 1,"incorrect pointer");
        assertTrue(((BPointer) p.peek()).getPage() == 8 && ((BPointer) p.poll()).getOffset() == 2,"incorrect pointer");
        assertTrue(((BPointer) p.peek()).getPage() == 9 && ((BPointer) p.poll()).getOffset() == 0,"incorrect pointer");

        p =  falsePages[1].getPointers();
        assertTrue(((BPointer) p.peek()).getPage() == 9 && ((BPointer) p.poll()).getOffset() == 1,"incorrect pointer");

        overflowPage[] truePages = new overflowPage[2];

        truePages[0] = Utilities.deserializeOverflow("TreeTesting_Boolean_true_0");
        assertEquals("TreeTesting_Boolean_true_1", truePages[0].getNext(),"overflow pages are not linked correctly");
        assertEquals(null, truePages[0].getPrev(),"overflow pages are not linked correctly");

        truePages[1] = Utilities.deserializeOverflow("TreeTesting_Boolean_true_1");
        assertEquals(null, truePages[1].getNext(),"overflow pages are not linked correctly");
        assertEquals("TreeTesting_Boolean_true_0", truePages[1].getPrev(),"overflow pages are not linked correctly");

        //sizes are correct
        assertEquals(3,truePages[0].size(),"incorrect overflow page size");
        assertEquals(2,truePages[1].size(),"incorrect overflow page size");

        //pointers are correct
        p =  truePages[0].getPointers();
        assertTrue(((BPointer) p.peek()).getPage() == 10 && ((BPointer) p.poll()).getOffset() == 0,"incorrect pointer");
        assertTrue(((BPointer) p.peek()).getPage() == 10 && ((BPointer) p.poll()).getOffset() == 1,"incorrect pointer");
        assertTrue(((BPointer) p.peek()).getPage() == 10 && ((BPointer) p.poll()).getOffset() == 2,"incorrect pointer");

        p =  truePages[1].getPointers();
        assertTrue(((BPointer) p.peek()).getPage() == 11 && ((BPointer) p.poll()).getOffset() == 0,"incorrect pointer");
        assertTrue(((BPointer) p.peek()).getPage() == 11 && ((BPointer) p.poll()).getOffset() == 1,"incorrect pointer");

    }

    @Test
    public void v_deleteAllOverflow(){
        File dir = new File("data//overflow_Pages");

        for(File file: dir.listFiles()) //clear pages from before
            if (!file.isDirectory())
                file.delete();

        //create some pages
        for (int i = 0; i < 250; i++) overflowPage.insert("dummy",new BPointer(i/3,i%3));

        //then delete them all
        overflowPage.destroyAllPages("dummy");

        assertEquals(0,dir.listFiles().length,"failed to destroy all pages");

    }

    //===================================HELPERS======================================
    @Ignore
    public static void insert(Object integer, Object Double,Object string, String date, Object Boolean, int squareSide ) throws DBAppException, ParseException {
        Hashtable<String, Object> tuple = new Hashtable<String, Object>();
        tuple.put("Integer",integer);
        tuple.put("Double",Double);
        tuple.put("String", string);
        tuple.put("Date",new SimpleDateFormat("yyyy-MM-dd").parse(date));
        tuple.put("Boolean",Boolean);

        Polygon temp = new Polygon(); //square with side length 1
        temp.addPoint(0,0);temp.addPoint(squareSide,0);temp.addPoint(0,squareSide);
        temp.addPoint(squareSide,squareSide);

        tuple.put("Polygon",new myPolygon(temp));

        DB.insertIntoTable("TreeTesting",tuple);
    }

    @Ignore
    public static void initialScenario() throws DBAppException, ParseException {
        insert(1,0.9,"A", "2020-03-01", false, 1);
        insert(25,25.9,"K", "2020-03-25", true, 25);
        insert(16,16.3,"I", "2020-03-16", true, 16);
        insert(9,9.8,"C", "2020-03-09", false, 9);
        insert(4,4.6,"B", "2020-03-04", false, 4);
    }

    @Ignore
    public static void insertAll() throws DBAppException, ParseException {
        initialScenario();
        insert(20,20.1,"J", "2020-03-20", true, 20);
        insert(13,13.2,"G", "2020-03-13", true, 13);
        insert(15,15.7,"H", "2020-03-15", true, 15);
        insert(10,10.56,"D", "2020-03-10", false, 10);
        insert(11,11.23,"E", "2020-03-11", false, 11);
        insert(12,12.49,"F", "2020-03-12", true, 12);
    }
    @Ignore
    public static void initializeTestProperties(){
        Properties p=new Properties();
        p.setProperty("MaximumRowsCountinPage","3");
        p.setProperty("NodeSize","3");
        p.setProperty("nextNodeId","1");

        try {
            p.store(new FileWriter("config//DBApp.properties"),"Database engine properties");
        }
        catch (Exception e) {
            System.out.println("Failed to write file!");
        }

    }

    @AfterClass
    public static void clearData() throws IOException {

        File dir = new File("config//DBApp.properties");
        dir.delete(); //delete properties file

        dir = new File("data"); //delete pages, tables, and metadata.

        for(File file: dir.listFiles())
            if (!file.isDirectory())
                file.delete();

        dir = new File("data//overflow_Pages"); //delete all overflow pages

        for(File file: dir.listFiles())
            if (!file.isDirectory())
                file.delete();

        dir = new File("data//BPlus//B+_Nodes"); //delete all BPlus tree nodes

        for(File file: dir.listFiles())
            if (!file.isDirectory())
                file.delete();

        dir = new File("data//BPlus//Trees"); //delete BPlus trees

        for(File file: dir.listFiles())
            if (!file.isDirectory())
                file.delete();


        dir = new File("data//overflow_Pages//.gitkeep");
        dir.createNewFile();
        dir = new File("data//BPlus//B+_Nodes//.gitkeep");
        dir.createNewFile();
        dir = new File("data//BPlus//Trees//.gitkeep");
        dir.createNewFile();

    }


}