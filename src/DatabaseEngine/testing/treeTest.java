package DatabaseEngine.testing;

import DatabaseEngine.*;
import DatabaseEngine.BPlus.BPTExternal;
import DatabaseEngine.BPlus.BPTInternal;
import DatabaseEngine.BPlus.BPlusTree;
import DatabaseEngine.BPlus.BPointer;
import DatabaseEngine.R.RExternal;
import DatabaseEngine.R.RInternal;
import DatabaseEngine.R.RTree;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.awt.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


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

        dir = new File("data//R//Trees"); //delete BPlus trees

        for(File file: dir.listFiles())
            if (!file.isDirectory())
                file.delete();

        dir = new File("data//R//R_Nodes"); //delete all BPlus tree nodes

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
        Assert.assertTrue(treeExists);
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
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());


        Assert.assertEquals("incorrect node size", 3, left.getSize());
        Assert.assertEquals("incorrect node size", 3, leftVals.size());
        Assert.assertEquals("incorrect node size", 3, leftPointers.size());

        Assert.assertEquals("incorrect node size", 2, right.getSize());
        Assert.assertEquals("incorrect node size", 2, rightVals.size());
        Assert.assertEquals("incorrect node size", 2, rightPointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", left.getNext(), rootPointers.get(1));
        Assert.assertNull("leaves are linked incorrectly", right.getNext());
        Assert.assertNull(right.getNext());

        //values are correct
        Assert.assertEquals("incorrect value", 16, (int) rootVals.get(0));
        Assert.assertEquals("incorrect value", 1, (int) leftVals.get(0));
        Assert.assertEquals("incorrect value", 4, (int) leftVals.get(1));
        Assert.assertEquals("incorrect value", 9, (int) leftVals.get(2));
        Assert.assertEquals("incorrect value", 16, (int) rightVals.get(0));
        Assert.assertEquals("incorrect value", 25, (int) rightVals.get(1));

        //pointers are correct
        Assert.assertTrue("incorrect pointer", leftPointers.get(0).getPage() == 1 && leftPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", leftPointers.get(1).getPage() == 1 && leftPointers.get(1).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", leftPointers.get(2).getPage() == 1 && leftPointers.get(2).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", rightPointers.get(0).getPage() == 2 && rightPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", rightPointers.get(1).getPage() == 2 && rightPointers.get(1).getOffset() == 1);
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
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());


        Assert.assertEquals("incorrect node size", 3, left.getSize());
        Assert.assertEquals("incorrect node size", 3, leftVals.size());
        Assert.assertEquals("incorrect node size", 3, leftPointers.size());

        Assert.assertEquals("incorrect node size", 3, right.getSize());
        Assert.assertEquals("incorrect node size", 3, rightVals.size());
        Assert.assertEquals("incorrect node size", 3, rightPointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", left.getNext(), rootPointers.get(1));
        Assert.assertNull("leaves are linked incorrectly", right.getNext());

        //values are correct
        Assert.assertEquals("incorrect value", 16, (int) rootVals.get(0));
        Assert.assertEquals("incorrect value", 1, (int) leftVals.get(0));
        Assert.assertEquals("incorrect value", 4, (int) leftVals.get(1));
        Assert.assertEquals("incorrect value", 9, (int) leftVals.get(2));
        Assert.assertEquals("incorrect value", 16, (int) rightVals.get(0));
        Assert.assertEquals("incorrect value", 20, (int) rightVals.get(1));
        Assert.assertEquals("incorrect value", 25, (int) rightVals.get(2));


        //pointers are correct
        Assert.assertTrue("incorrect pointer", leftPointers.get(0).getPage() == 1 && leftPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", leftPointers.get(1).getPage() == 1 && leftPointers.get(1).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", leftPointers.get(2).getPage() == 1 && leftPointers.get(2).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", rightPointers.get(0).getPage() == 2 && rightPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", rightPointers.get(1).getPage() == 2 && rightPointers.get(1).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", rightPointers.get(2).getPage() == 2 && rightPointers.get(2).getOffset() == 2);

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
        Assert.assertEquals("incorrect node size", 2, root.getSize());
        Assert.assertEquals("incorrect node size", 2, rootVals.size());
        Assert.assertEquals("incorrect node size", 3, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, left.getSize());
        Assert.assertEquals("incorrect node size", 2, leftVals.size());
        Assert.assertEquals("incorrect node size", 2, leftPointers.size());

        Assert.assertEquals("incorrect node size", 2, mid.getSize());
        Assert.assertEquals("incorrect node size", 2, midVals.size());
        Assert.assertEquals("incorrect node size", 2, midPointers.size());

        Assert.assertEquals("incorrect node size", 3, right.getSize());
        Assert.assertEquals("incorrect node size", 3, rightVals.size());
        Assert.assertEquals("incorrect node size", 3, rightPointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", left.getNext(), rootPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", mid.getNext(), rootPointers.get(2));
        Assert.assertNull("leaves are linked incorrectly", right.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 9, (int) rootVals.get(0));
        Assert.assertEquals("incorrect value", 16, (int) rootVals.get(1));

        Assert.assertEquals("incorrect value", 1, (int) leftVals.get(0));
        Assert.assertEquals("incorrect value", 4, (int) leftVals.get(1));

        Assert.assertEquals("incorrect value", 9, (int) midVals.get(0));
        Assert.assertEquals("incorrect value", 13, (int) midVals.get(1));

        Assert.assertEquals("incorrect value", 16, (int) rightVals.get(0));
        Assert.assertEquals("incorrect value", 20, (int) rightVals.get(1));
        Assert.assertEquals("incorrect value", 25, (int) rightVals.get(2));


        //pointers are correct
        Assert.assertTrue("incorrect pointer", leftPointers.get(0).getPage() == 1 && leftPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", leftPointers.get(1).getPage() == 1 && leftPointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", midPointers.get(0).getPage() == 1 && midPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", midPointers.get(1).getPage() == 2 && midPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", rightPointers.get(0).getPage() == 2 && rightPointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", rightPointers.get(1).getPage() == 2 && rightPointers.get(1).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", rightPointers.get(2).getPage() == 3 && rightPointers.get(2).getOffset() == 0);

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
        Assert.assertEquals("incorrect node size", 2, root.getSize());
        Assert.assertEquals("incorrect node size", 2, rootVals.size());
        Assert.assertEquals("incorrect node size", 3, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, left.getSize());
        Assert.assertEquals("incorrect node size", 2, leftVals.size());
        Assert.assertEquals("incorrect node size", 2, leftPointers.size());

        Assert.assertEquals("incorrect node size", 3, mid.getSize());
        Assert.assertEquals("incorrect node size", 3, midVals.size());
        Assert.assertEquals("incorrect node size", 3, midPointers.size());

        Assert.assertEquals("incorrect node size", 3, right.getSize());
        Assert.assertEquals("incorrect node size", 3, rightVals.size());
        Assert.assertEquals("incorrect node size", 3, rightPointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", left.getNext(), rootPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", mid.getNext(), rootPointers.get(2));
        Assert.assertNull("leaves are linked incorrectly", right.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 9, (int) rootVals.get(0));
        Assert.assertEquals("incorrect value", 16, (int) rootVals.get(1));

        Assert.assertEquals("incorrect value", 1, (int) leftVals.get(0));
        Assert.assertEquals("incorrect value", 4, (int) leftVals.get(1));

        Assert.assertEquals("incorrect value", 9, (int) midVals.get(0));
        Assert.assertEquals("incorrect value", 13, (int) midVals.get(1));
        Assert.assertEquals("incorrect value", 15, (int) midVals.get(2));


        Assert.assertEquals("incorrect value", 16, (int) rightVals.get(0));
        Assert.assertEquals("incorrect value", 20, (int) rightVals.get(1));
        Assert.assertEquals("incorrect value", 25, (int) rightVals.get(2));


        //pointers are correct
        Assert.assertTrue("incorrect pointer", leftPointers.get(0).getPage() == 1 && leftPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", leftPointers.get(1).getPage() == 1 && leftPointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", midPointers.get(0).getPage() == 1 && midPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", midPointers.get(1).getPage() == 2 && midPointers.get(1).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", midPointers.get(2).getPage() == 2 && midPointers.get(2).getOffset() == 1);


        Assert.assertTrue("incorrect pointer", rightPointers.get(0).getPage() == 2 && rightPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", rightPointers.get(1).getPage() == 3 && rightPointers.get(1).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", rightPointers.get(2).getPage() == 3 && rightPointers.get(2).getOffset() == 1);
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
        Assert.assertEquals("incorrect node size", 3, root.getSize());
        Assert.assertEquals("incorrect node size", 3, rootVals.size());
        Assert.assertEquals("incorrect node size", 4, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, one.getSize());
        Assert.assertEquals("incorrect node size", 2, oneVals.size());
        Assert.assertEquals("incorrect node size", 2, onePointers.size());

        Assert.assertEquals("incorrect node size", 2, two.getSize());
        Assert.assertEquals("incorrect node size", 2, twoVals.size());
        Assert.assertEquals("incorrect node size", 2, twoPointers.size());

        Assert.assertEquals("incorrect node size", 2, three.getSize());
        Assert.assertEquals("incorrect node size", 2, threeVals.size());
        Assert.assertEquals("incorrect node size", 2, threePointers.size());

        Assert.assertEquals("incorrect node size", 3, four.getSize());
        Assert.assertEquals("incorrect node size", 3, fourVals.size());
        Assert.assertEquals("incorrect node size", 3, fourPointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", one.getNext(), rootPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", two.getNext(), rootPointers.get(2));
        Assert.assertEquals("leaves are linked incorrectly", three.getNext(), rootPointers.get(3));
        Assert.assertNull(four.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 9, (int) rootVals.get(0));
        Assert.assertEquals("incorrect value", 13, (int) rootVals.get(1));
        Assert.assertEquals("incorrect value", 16, (int) rootVals.get(2));

        Assert.assertEquals("incorrect value", 1, (int) oneVals.get(0));
        Assert.assertEquals("incorrect value", 4, (int) oneVals.get(1));

        Assert.assertEquals("incorrect value", 9, (int) twoVals.get(0));
        Assert.assertEquals("incorrect value", 10, (int) twoVals.get(1));

        Assert.assertEquals(13, (int) threeVals.get(0));
        Assert.assertEquals(15, (int) threeVals.get(1));

        Assert.assertEquals("incorrect value", 16, (int) fourVals.get(0));
        Assert.assertEquals("incorrect value", 20, (int) fourVals.get(1));
        Assert.assertEquals("incorrect value", 25, (int) fourVals.get(2));


        //pointers are correct
        Assert.assertTrue("incorrect pointer", onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", threePointers.get(1).getPage() == 2 && threePointers.get(1).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", fourPointers.get(0).getPage() == 3 && fourPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fourPointers.get(1).getPage() == 3 && fourPointers.get(1).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", fourPointers.get(2).getPage() == 3 && fourPointers.get(2).getOffset() == 2);
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
        Assert.assertEquals("incorrect node size", 3, root.getSize());
        Assert.assertEquals("incorrect node size", 3, rootVals.size());
        Assert.assertEquals("incorrect node size", 4, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, one.getSize());
        Assert.assertEquals("incorrect node size", 2, oneVals.size());
        Assert.assertEquals("incorrect node size", 2, onePointers.size());

        Assert.assertEquals("incorrect node size", 3, two.getSize());
        Assert.assertEquals("incorrect node size", 3, twoVals.size());
        Assert.assertEquals("incorrect node size", 3, twoPointers.size());

        Assert.assertEquals("incorrect node size", 2, three.getSize());
        Assert.assertEquals("incorrect node size", 2, threeVals.size());
        Assert.assertEquals("incorrect node size", 2, threePointers.size());

        Assert.assertEquals("incorrect node size", 3, four.getSize());
        Assert.assertEquals("incorrect node size", 3, fourVals.size());
        Assert.assertEquals("incorrect node size", 3, fourPointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", one.getNext(), rootPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", two.getNext(), rootPointers.get(2));
        Assert.assertEquals("leaves are linked incorrectly", three.getNext(), rootPointers.get(3));
        Assert.assertNull(four.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 9, (int) rootVals.get(0));
        Assert.assertEquals("incorrect value", 13, (int) rootVals.get(1));
        Assert.assertEquals("incorrect value", 16, (int) rootVals.get(2));

        Assert.assertEquals("incorrect value", 1, (int) oneVals.get(0));
        Assert.assertEquals("incorrect value", 4, (int) oneVals.get(1));

        Assert.assertEquals("incorrect value", 9, (int) twoVals.get(0));
        Assert.assertEquals("incorrect value", 10, (int) twoVals.get(1));
        Assert.assertEquals("incorrect value", 11, (int) twoVals.get(2));

        Assert.assertEquals("incorrect value", 13, (int) threeVals.get(0));
        Assert.assertEquals("incorrect value", 15, (int) threeVals.get(1));

        Assert.assertEquals("incorrect value", 16, (int) fourVals.get(0));
        Assert.assertEquals("incorrect value", 20, (int) fourVals.get(1));
        Assert.assertEquals("incorrect value", 25, (int) fourVals.get(2));


        //pointers are correct
        Assert.assertTrue("incorrect pointer", onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", twoPointers.get(2).getPage() == 2 && twoPointers.get(2).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", threePointers.get(1).getPage() == 3 && threePointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", fourPointers.get(0).getPage() == 3 && fourPointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", fourPointers.get(1).getPage() == 3 && fourPointers.get(1).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", fourPointers.get(2).getPage() == 4 && fourPointers.get(2).getOffset() == 0);
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
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, left.getSize());
        Assert.assertEquals("incorrect node size", 2, leftVals.size());
        Assert.assertEquals("incorrect node size", 3, leftPointers.size());

        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 2, rightPointers.size());

        Assert.assertEquals("incorrect node size", 2, one.getSize());
        Assert.assertEquals("incorrect node size", 2, oneVals.size());
        Assert.assertEquals("incorrect node size", 2, onePointers.size());

        Assert.assertEquals("incorrect node size", 2, two.getSize());
        Assert.assertEquals("incorrect node size", 2, twoVals.size());
        Assert.assertEquals("incorrect node size", 2, twoPointers.size());

        Assert.assertEquals("incorrect node size", 2, three.getSize());
        Assert.assertEquals("incorrect node size", 2, threeVals.size());
        Assert.assertEquals("incorrect node size", 2, threePointers.size());

        Assert.assertEquals("incorrect node size", 2, four.getSize());
        Assert.assertEquals("incorrect node size", 2, fourVals.size());
        Assert.assertEquals("incorrect node size", 2, fourPointers.size());

        Assert.assertEquals("incorrect node size", 3, five.getSize());
        Assert.assertEquals("incorrect node size", 3, fiveVals.size());
        Assert.assertEquals("incorrect node size", 3, fivePointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", one.getNext(), leftPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", two.getNext(), leftPointers.get(2));
        Assert.assertEquals("leaves are linked incorrectly", three.getNext(), rightPointers.get(0));
        Assert.assertEquals("leaves are linked incorrectly", four.getNext(), rightPointers.get(1));
        Assert.assertNull(five.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 13, (int) rootVals.get(0));

        Assert.assertEquals("incorrect value", 9, (int) leftVals.get(0));
        Assert.assertEquals("incorrect value", 11, (int) leftVals.get(1));

        Assert.assertEquals("incorrect value", 16, (int) rightVals.get(0));

        Assert.assertEquals("incorrect value", 1, (int) oneVals.get(0));
        Assert.assertEquals("incorrect value", 4, (int) oneVals.get(1));

        Assert.assertEquals("incorrect value", 9, (int) twoVals.get(0));
        Assert.assertEquals("incorrect value", 10, (int) twoVals.get(1));

        Assert.assertEquals("incorrect value", 11, (int) threeVals.get(0));
        Assert.assertEquals("incorrect value", 12, (int) threeVals.get(1));

        Assert.assertEquals("incorrect value", 13, (int) fourVals.get(0));
        Assert.assertEquals("incorrect value", 15, (int) fourVals.get(1));

        Assert.assertEquals("incorrect value", 16, (int) fiveVals.get(0));
        Assert.assertEquals("incorrect value", 20, (int) fiveVals.get(1));
        Assert.assertEquals("incorrect value", 25, (int) fiveVals.get(2));



        //pointers are correct
        Assert.assertTrue("incorrect pointer", onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", threePointers.get(1).getPage() == 2 && threePointers.get(1).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", fourPointers.get(0).getPage() == 3 && fourPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fourPointers.get(1).getPage() == 3 && fourPointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", fivePointers.get(0).getPage() == 3 && fivePointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", fivePointers.get(1).getPage() == 4 && fivePointers.get(1).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fivePointers.get(2).getPage() == 4 && fivePointers.get(2).getOffset() == 1);
    }

    //================================TREE_OVERFLOW===================================

    @Test
    public void I_overflowInsert() throws DBAppException, ParseException {
        for (int i = 0; i < 9; i++)  //3 full pages
            insert(13,13.2,"G", "2020-03-13", true, 13);

        String path = "data//overflow_Pages//overflow_TreeTesting_Integer_13_0.class";
        Assert.assertTrue(new File(path).isFile()); //first page

        overflowPage[] pages = new overflowPage[3];

        pages[0] = Utilities.deserializeOverflow("TreeTesting_Integer_13_0");
        Assert.assertEquals("overflow pages are not linked correctly", "TreeTesting_Integer_13_1", pages[0].getNext());
        Assert.assertNull("overflow pages are not linked correctly", pages[0].getPrev());

        pages[1] = Utilities.deserializeOverflow("TreeTesting_Integer_13_1");
        Assert.assertEquals("overflow pages are not linked correctly", "TreeTesting_Integer_13_2", pages[1].getNext());
        Assert.assertEquals("overflow pages are not linked correctly", "TreeTesting_Integer_13_0", pages[1].getPrev());

        pages[2] = Utilities.deserializeOverflow("TreeTesting_Integer_13_2");
        Assert.assertNull("overflow pages are not linked correctly", pages[2].getNext());
        Assert.assertEquals("overflow pages are not linked correctly", "TreeTesting_Integer_13_1", pages[2].getPrev());

        //sizes
        Assert.assertEquals("page size is incorrect", 3, pages[0].size());
        Assert.assertEquals("page size is incorrect", 3, pages[1].size());
        Assert.assertEquals("page size is incorrect", 3, pages[2].size());

        //correct pointers
        for (int i = 0; i < 3; i++) {
            Queue<Pointer> curPage = pages[i].getPointers();
            int j = 0;
            while (!curPage.isEmpty()){
                BPointer curPointer = (BPointer) curPage.poll();

                Assert.assertEquals("overflow pointer offeset is incorrect", 2 - j, curPointer.getOffset());
                Assert.assertEquals("overflow pointer page is incorrect", 5 - i, curPointer.getPage());

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
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, left.getSize());
        Assert.assertEquals("incorrect node size", 2, leftVals.size());
        Assert.assertEquals("incorrect node size", 3, leftPointers.size());

        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 2, rightPointers.size());

        Assert.assertEquals("incorrect node size", 2, one.getSize());
        Assert.assertEquals("incorrect node size", 2, oneVals.size());
        Assert.assertEquals("incorrect node size", 2, onePointers.size());

        Assert.assertEquals("incorrect node size", 2, two.getSize());
        Assert.assertEquals("incorrect node size", 2, twoVals.size());
        Assert.assertEquals("incorrect node size", 2, twoPointers.size());

        Assert.assertEquals("incorrect node size", 2, three.getSize());
        Assert.assertEquals("incorrect node size", 2, threeVals.size());
        Assert.assertEquals("incorrect node size", 2, threePointers.size());

        Assert.assertEquals("incorrect node size", 2, four.getSize());
        Assert.assertEquals("incorrect node size", 2, fourVals.size());
        Assert.assertEquals("incorrect node size", 2, fourPointers.size());

        Assert.assertEquals("incorrect node size", 3, five.getSize());
        Assert.assertEquals("incorrect node size", 3, fiveVals.size());
        Assert.assertEquals("incorrect node size", 3, fivePointers.size());


        //nodes are linked correctly
        Assert.assertEquals(one.getNext(), leftPointers.get(1));
        Assert.assertEquals(two.getNext(), leftPointers.get(2));
        Assert.assertEquals(three.getNext(), rightPointers.get(0));
        Assert.assertEquals(four.getNext(), rightPointers.get(1));
        Assert.assertNull(five.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 13, (int) rootVals.get(0));

        Assert.assertEquals("incorrect value", 9, (int) leftVals.get(0));
        Assert.assertEquals("incorrect value", 11, (int) leftVals.get(1));

        Assert.assertEquals("incorrect value", 16, (int) rightVals.get(0));

        Assert.assertEquals("incorrect value", 1, (int) oneVals.get(0));
        Assert.assertEquals("incorrect value", 4, (int) oneVals.get(1));

        Assert.assertEquals("incorrect value", 9, (int) twoVals.get(0));
        Assert.assertEquals("incorrect value", 10, (int) twoVals.get(1));

        Assert.assertEquals("incorrect value", 11, (int) threeVals.get(0));
        Assert.assertEquals("incorrect value", 12, (int) threeVals.get(1));

        Assert.assertEquals("incorrect value", 13, (int) fourVals.get(0));
        Assert.assertEquals("incorrect value", 15, (int) fourVals.get(1));

        Assert.assertEquals("incorrect value", 16, (int) fiveVals.get(0));
        Assert.assertEquals("incorrect value", 20, (int) fiveVals.get(1));
        Assert.assertEquals("incorrect value", 25, (int) fiveVals.get(2));



        //pointers are correct
        Assert.assertTrue("incorrect pointer", onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", threePointers.get(1).getPage() == 2 && threePointers.get(1).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", fourPointers.get(0).getPage() == 6 && fourPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fourPointers.get(1).getPage() == 6 && fourPointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", fivePointers.get(0).getPage() == 6 && fivePointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", fivePointers.get(1).getPage() == 7 && fivePointers.get(1).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fivePointers.get(2).getPage() == 7 && fivePointers.get(2).getOffset() == 1);

    }

    //===============================TREE_DELETE===================================
    @Test
    public void J_testDelete13() throws DBAppException {
        Hashtable<String, Object> tuple = new Hashtable<>();
        tuple.put("Integer",13);

        DB.deleteFromTable("TreeTesting", tuple); //delete 13 with its 9 duplicates

        File overflow = new File("data//overflow_Pages");
        Assert.assertEquals("overflow pages still exist after deletion", 0,overflow.listFiles().length);

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
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, left.getSize());
        Assert.assertEquals("incorrect node size", 2, leftVals.size());
        Assert.assertEquals("incorrect node size", 3, leftPointers.size());

        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 2, rightPointers.size());

        Assert.assertEquals("incorrect node size", 2, one.getSize());
        Assert.assertEquals("incorrect node size", 2, oneVals.size());
        Assert.assertEquals("incorrect node size", 2, onePointers.size());

        Assert.assertEquals("incorrect node size", 2, two.getSize());
        Assert.assertEquals("incorrect node size", 2, twoVals.size());
        Assert.assertEquals("incorrect node size", 2, twoPointers.size());

        Assert.assertEquals("incorrect node size", 2, three.getSize());
        Assert.assertEquals("incorrect node size", 2, threeVals.size());
        Assert.assertEquals("incorrect node size", 2, threePointers.size());

        Assert.assertEquals("incorrect node size", 1, four.getSize());
        Assert.assertEquals("incorrect node size", 1, fourVals.size());
        Assert.assertEquals("incorrect node size", 1, fourPointers.size());

        Assert.assertEquals("incorrect node size", 3, five.getSize());
        Assert.assertEquals("incorrect node size", 3, fiveVals.size());
        Assert.assertEquals("incorrect node size", 3, fivePointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", one.getNext(), leftPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", two.getNext(), leftPointers.get(2));
        Assert.assertEquals("leaves are linked incorrectly", three.getNext(), rightPointers.get(0));
        Assert.assertEquals("leaves are linked incorrectly", four.getNext(), rightPointers.get(1));
        Assert.assertNull("leaves are linked incorrectly", five.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 15, (int) rootVals.get(0));

        Assert.assertEquals("incorrect value", 9, (int) leftVals.get(0));
        Assert.assertEquals("incorrect value", 11, (int) leftVals.get(1));

        Assert.assertEquals("incorrect value", 16, (int) rightVals.get(0));

        Assert.assertEquals("incorrect value", 1, (int) oneVals.get(0));
        Assert.assertEquals("incorrect value", 4, (int) oneVals.get(1));

        Assert.assertEquals("incorrect value", 9, (int) twoVals.get(0));
        Assert.assertEquals("incorrect value", 10, (int) twoVals.get(1));

        Assert.assertEquals("incorrect value", 11, (int) threeVals.get(0));
        Assert.assertEquals("incorrect value", 12, (int) threeVals.get(1));

        Assert.assertEquals("incorrect value", 15, (int) fourVals.get(0));

        Assert.assertEquals("incorrect value", 16, (int) fiveVals.get(0));
        Assert.assertEquals("incorrect value", 20, (int) fiveVals.get(1));
        Assert.assertEquals("incorrect value", 25, (int) fiveVals.get(2));



        //pointers are correct
        Assert.assertTrue("incorrect pointer", onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", threePointers.get(1).getPage() == 2 && threePointers.get(1).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", fourPointers.get(0).getPage() == 6 && fourPointers.get(0).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", fivePointers.get(0).getPage() == 6 && fivePointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", fivePointers.get(1).getPage() == 7 && fivePointers.get(1).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fivePointers.get(2).getPage() == 7 && fivePointers.get(2).getOffset() == 1);

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
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, left.getSize());
        Assert.assertEquals("incorrect node size", 2, leftVals.size());
        Assert.assertEquals("incorrect node size", 3, leftPointers.size());

        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 2, rightPointers.size());

        Assert.assertEquals("incorrect node size", 2, one.getSize());
        Assert.assertEquals("incorrect node size", 2, oneVals.size());
        Assert.assertEquals("incorrect node size", 2, onePointers.size());

        Assert.assertEquals("incorrect node size", 2, two.getSize());
        Assert.assertEquals("incorrect node size", 2, twoVals.size());
        Assert.assertEquals("incorrect node size", 2, twoPointers.size());

        Assert.assertEquals("incorrect node size", 2, three.getSize());
        Assert.assertEquals("incorrect node size", 2, threeVals.size());
        Assert.assertEquals("incorrect node size", 2, threePointers.size());

        Assert.assertEquals("incorrect node size", 1, four.getSize());
        Assert.assertEquals("incorrect node size", 1, fourVals.size());
        Assert.assertEquals("incorrect node size", 1, fourPointers.size());

        Assert.assertEquals("incorrect node size", 2, five.getSize());
        Assert.assertEquals("incorrect node size", 2, fiveVals.size());
        Assert.assertEquals("incorrect node size", 2, fivePointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", one.getNext(), leftPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", two.getNext(), leftPointers.get(2));
        Assert.assertEquals("leaves are linked incorrectly", three.getNext(), rightPointers.get(0));
        Assert.assertEquals("leaves are linked incorrectly", four.getNext(), rightPointers.get(1));
        Assert.assertNull("leaves are linked incorrectly", five.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 16, (int) rootVals.get(0));

        Assert.assertEquals("incorrect value", 9, (int) leftVals.get(0));
        Assert.assertEquals("incorrect value", 11, (int) leftVals.get(1));

        Assert.assertEquals("incorrect value", 20, (int) rightVals.get(0));

        Assert.assertEquals("incorrect value", 1, (int) oneVals.get(0));
        Assert.assertEquals("incorrect value", 4, (int) oneVals.get(1));

        Assert.assertEquals("incorrect value", 9, (int) twoVals.get(0));
        Assert.assertEquals("incorrect value", 10, (int) twoVals.get(1));

        Assert.assertEquals("incorrect value", 11, (int) threeVals.get(0));
        Assert.assertEquals("incorrect value", 12, (int) threeVals.get(1));

        Assert.assertEquals("incorrect value", 16, (int) fourVals.get(0));

        Assert.assertEquals("incorrect value", 20, (int) fiveVals.get(0));
        Assert.assertEquals("incorrect value", 25, (int) fiveVals.get(1));



        //pointers are correct
        Assert.assertTrue("incorrect pointer", onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", threePointers.get(1).getPage() == 2 && threePointers.get(1).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", fourPointers.get(0).getPage() == 6 && fourPointers.get(0).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", fivePointers.get(0).getPage() == 7 && fivePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fivePointers.get(1).getPage() == 7 && fivePointers.get(1).getOffset() == 1);
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
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        Assert.assertEquals("incorrect node size", 1, left.getSize());
        Assert.assertEquals("incorrect node size", 1, leftVals.size());
        Assert.assertEquals("incorrect node size", 2, leftPointers.size());

        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 2, rightPointers.size());

        Assert.assertEquals("incorrect node size", 2, one.getSize());
        Assert.assertEquals("incorrect node size", 2, oneVals.size());
        Assert.assertEquals("incorrect node size", 2, onePointers.size());

        Assert.assertEquals("incorrect node size", 2, two.getSize());
        Assert.assertEquals("incorrect node size", 2, twoVals.size());
        Assert.assertEquals("incorrect node size", 2, twoPointers.size());

        Assert.assertEquals("incorrect node size", 2, three.getSize());
        Assert.assertEquals("incorrect node size", 2, threeVals.size());
        Assert.assertEquals("incorrect node size", 2, threePointers.size());

        Assert.assertEquals("incorrect node size", 1, four.getSize());
        Assert.assertEquals("incorrect node size", 1, fourVals.size());
        Assert.assertEquals("incorrect node size", 1, fourPointers.size());

        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", one.getNext(), leftPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", two.getNext(), rightPointers.get(0));
        Assert.assertEquals("leaves are linked incorrectly", three.getNext(), rightPointers.get(1));
        Assert.assertNull(four.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 11, (int) rootVals.get(0));

        Assert.assertEquals("incorrect value", 9, (int) leftVals.get(0));

        Assert.assertEquals("incorrect value", 25, (int) rightVals.get(0));

        Assert.assertEquals("incorrect value", 1, (int) oneVals.get(0));
        Assert.assertEquals("incorrect value", 4, (int) oneVals.get(1));

        Assert.assertEquals("incorrect value", 9, (int) twoVals.get(0));
        Assert.assertEquals("incorrect value", 10, (int) twoVals.get(1));

        Assert.assertEquals("incorrect value", 11, (int) threeVals.get(0));
        Assert.assertEquals("incorrect value", 12, (int) threeVals.get(1));

        Assert.assertEquals("incorrect value", 25, (int) fourVals.get(0));


        //pointers are correct
        Assert.assertTrue("incorrect pointer", onePointers.get(0).getPage() == 1 && onePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", onePointers.get(1).getPage() == 1 && onePointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", twoPointers.get(0).getPage() == 1 && twoPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", twoPointers.get(1).getPage() == 2 && twoPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", threePointers.get(0).getPage() == 2 && threePointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", threePointers.get(1).getPage() == 2 && threePointers.get(1).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", fourPointers.get(0).getPage() == 7 && fourPointers.get(0).getOffset() == 0);
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
        Assert.assertEquals("incorrect node size", 2, root.getSize());
        Assert.assertEquals("incorrect node size", 2, rootVals.size());
        Assert.assertEquals("incorrect node size", 3, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, left.getSize());
        Assert.assertEquals("incorrect node size", 2, leftVals.size());
        Assert.assertEquals("incorrect node size", 2, leftPointers.size());

        Assert.assertEquals("incorrect node size", 2, mid.getSize());
        Assert.assertEquals("incorrect node size", 2, midVals.size());
        Assert.assertEquals("incorrect node size", 2, midPointers.size());

        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 1, rightPointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", left.getNext(), rootPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", mid.getNext(), rootPointers.get(2));
        Assert.assertNull(right.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 9, (int) rootVals.get(0));
        Assert.assertEquals("incorrect value", 25, (int) rootVals.get(1));

        Assert.assertEquals("incorrect value", 1, (int) leftVals.get(0));
        Assert.assertEquals("incorrect value", 4, (int) leftVals.get(1));

        Assert.assertEquals("incorrect value", 9, (int) midVals.get(0));
        Assert.assertEquals("incorrect value", 10, (int) midVals.get(1));


        Assert.assertEquals("incorrect value", 25, (int) rightVals.get(0));


        //pointers are correct
        Assert.assertTrue("incorrect pointer", leftPointers.get(0).getPage() == 1 && leftPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", leftPointers.get(1).getPage() == 1 && leftPointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", midPointers.get(0).getPage() == 1 && midPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", midPointers.get(1).getPage() == 2 && midPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", rightPointers.get(0).getPage() == 7 && rightPointers.get(0).getOffset() == 0);

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
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        Assert.assertEquals("incorrect node size", 1, left.getSize());
        Assert.assertEquals("incorrect node size", 1, leftVals.size());
        Assert.assertEquals("incorrect node size", 1, leftPointers.size());


        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 1, rightPointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", left.getNext(), rootPointers.get(1));
        Assert.assertNull("leaves are linked incorrectly", right.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 4, (int) rootVals.get(0));

        Assert.assertEquals("incorrect value", 1, (int) leftVals.get(0));

        Assert.assertEquals("incorrect value", 4, (int) rightVals.get(0));


        //pointers are correct
        Assert.assertTrue("incorrect pointer", leftPointers.get(0).getPage() == 1 && leftPointers.get(0).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", rightPointers.get(0).getPage() == 1 && rightPointers.get(0).getOffset() == 1);

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
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 1, rootPointers.size());

        //nodes are linked correctly
        Assert.assertNull("leaves are linked incorrectly", root.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 4, (int) rootVals.get(0));


        //pointers are correct
        Assert.assertTrue("incorrect pointer", rootPointers.get(0).getPage() == 1 && rootPointers.get(0).getOffset() == 0);

    }

    @Test
    public void P_testEmptyTree() throws DBAppException {
        Hashtable<String, Object> tuple = new Hashtable<>();
        tuple.put("Integer",4);
        DB.deleteFromTable("TreeTesting", tuple);

        //----------------------------------TREE_STRUCTURE--------------------------------------

        BPlusTree<Integer> treeInt = Utilities.deserializeBPT("TreeTesting_Integer");
        Assert.assertNull("tree root should be null", treeInt.getRoot());
    }

    @Test
    public void Q_createAllIndices() throws DBAppException, ParseException {
        insertAll();

        DB.createBTreeIndex("TreeTesting","Double");
        DB.createBTreeIndex("TreeTesting","String");
        DB.createBTreeIndex("TreeTesting","Date");
        DB.createBTreeIndex("TreeTesting","Boolean");
        DB.createRTreeIndex("TreeTesting","Polygon");

        //trees exist
        Assert.assertTrue(new File("data//BPlus//Trees//BPlusTree_TreeTesting_Integer.class").isFile());
        Assert.assertTrue(new File("data//BPlus//Trees//BPlusTree_TreeTesting_Double.class").isFile());
        Assert.assertTrue(new File("data//BPlus//Trees//BPlusTree_TreeTesting_String.class").isFile());
        Assert.assertTrue(new File("data//BPlus//Trees//BPlusTree_TreeTesting_Date.class").isFile());
        Assert.assertTrue(new File("data//BPlus//Trees//BPlusTree_TreeTesting_Boolean.class").isFile());
        Assert.assertTrue(new File("data//R//Trees//RTree_TreeTesting_Polygon.class").isFile());
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
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, left.getSize());
        Assert.assertEquals("incorrect node size", 2, leftVals.size());
        Assert.assertEquals("incorrect node size", 3, leftPointers.size());

        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 2, rightPointers.size());

        Assert.assertEquals("incorrect node size", 2, one.getSize());
        Assert.assertEquals("incorrect node size", 2, oneVals.size());
        Assert.assertEquals("incorrect node size", 2, onePointers.size());

        Assert.assertEquals("incorrect node size", 2, two.getSize());
        Assert.assertEquals("incorrect node size", 2, twoVals.size());
        Assert.assertEquals("incorrect node size", 2, twoPointers.size());

        Assert.assertEquals("incorrect node size", 2, three.getSize());
        Assert.assertEquals("incorrect node size", 2, threeVals.size());
        Assert.assertEquals("incorrect node size", 2, threePointers.size());

        Assert.assertEquals("incorrect node size", 2, four.getSize());
        Assert.assertEquals("incorrect node size", 2, fourVals.size());
        Assert.assertEquals("incorrect node size", 2, fourPointers.size());

        Assert.assertEquals("incorrect node size", 3, five.getSize());
        Assert.assertEquals("incorrect node size", 3, fiveVals.size());
        Assert.assertEquals("incorrect node size", 3, fivePointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", one.getNext(), leftPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", two.getNext(), leftPointers.get(2));
        Assert.assertEquals("leaves are linked incorrectly", three.getNext(), rightPointers.get(0));
        Assert.assertEquals("leaves are linked incorrectly", four.getNext(), rightPointers.get(1));
        Assert.assertNull(five.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", 13.2, rootVals.get(0), 0.0);

        Assert.assertEquals("incorrect value", 9.8, leftVals.get(0), 0.0);
        Assert.assertEquals("incorrect value", 11.23, leftVals.get(1), 0.0);

        Assert.assertEquals("incorrect value", 16.3, rightVals.get(0), 0.0);

        Assert.assertEquals("incorrect value", 0.9, oneVals.get(0), 0.0);
        Assert.assertEquals("incorrect value", 4.6, oneVals.get(1), 0.0);

        Assert.assertEquals("incorrect value", 9.8, twoVals.get(0), 0.0);
        Assert.assertEquals("incorrect value", 10.56, twoVals.get(1), 0.0);

        Assert.assertEquals("incorrect value", 11.23, threeVals.get(0), 0.0);
        Assert.assertEquals("incorrect value", 12.49, threeVals.get(1), 0.0);

        Assert.assertEquals("incorrect value", 13.2, fourVals.get(0), 0.0);
        Assert.assertEquals("incorrect value", 15.7, fourVals.get(1), 0.0);

        Assert.assertEquals("incorrect value", 16.3, fiveVals.get(0), 0.0);
        Assert.assertEquals("incorrect value", 20.1, fiveVals.get(1), 0.0);
        Assert.assertEquals("incorrect value", 25.9, fiveVals.get(2), 0.0);



        //pointers are correct
        Assert.assertTrue("incorrect pointer", onePointers.get(0).getPage() == 8 && onePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", onePointers.get(1).getPage() == 8 && onePointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", twoPointers.get(0).getPage() == 8 && twoPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", twoPointers.get(1).getPage() == 9 && twoPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", threePointers.get(0).getPage() == 9 && threePointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", threePointers.get(1).getPage() == 9 && threePointers.get(1).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", fourPointers.get(0).getPage() == 10 && fourPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fourPointers.get(1).getPage() == 10 && fourPointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", fivePointers.get(0).getPage() == 10 && fivePointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", fivePointers.get(1).getPage() == 11 && fivePointers.get(1).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fivePointers.get(2).getPage() == 11 && fivePointers.get(2).getOffset() == 1);
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
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, left.getSize());
        Assert.assertEquals("incorrect node size", 2, leftVals.size());
        Assert.assertEquals("incorrect node size", 3, leftPointers.size());

        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 2, rightPointers.size());

        Assert.assertEquals("incorrect node size", 2, one.getSize());
        Assert.assertEquals("incorrect node size", 2, oneVals.size());
        Assert.assertEquals("incorrect node size", 2, onePointers.size());

        Assert.assertEquals("incorrect node size", 2, two.getSize());
        Assert.assertEquals("incorrect node size", 2, twoVals.size());
        Assert.assertEquals("incorrect node size", 2, twoPointers.size());

        Assert.assertEquals("incorrect node size", 2, three.getSize());
        Assert.assertEquals("incorrect node size", 2, threeVals.size());
        Assert.assertEquals("incorrect node size", 2, threePointers.size());

        Assert.assertEquals("incorrect node size", 2, four.getSize());
        Assert.assertEquals("incorrect node size", 2, fourVals.size());
        Assert.assertEquals("incorrect node size", 2, fourPointers.size());

        Assert.assertEquals("incorrect node size", 3, five.getSize());
        Assert.assertEquals("incorrect node size", 3, fiveVals.size());
        Assert.assertEquals("incorrect node size", 3, fivePointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", one.getNext(), leftPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", two.getNext(), leftPointers.get(2));
        Assert.assertEquals("leaves are linked incorrectly", three.getNext(), rightPointers.get(0));
        Assert.assertEquals("leaves are linked incorrectly", four.getNext(), rightPointers.get(1));
        Assert.assertNull(five.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", "G",rootVals.get(0));

        Assert.assertEquals("incorrect value", "C", leftVals.get(0));
        Assert.assertEquals("incorrect value", "E",leftVals.get(1));

        Assert.assertEquals("incorrect value", "I", rightVals.get(0));

        Assert.assertEquals("incorrect value", "A",oneVals.get(0));
        Assert.assertEquals("incorrect value", "B", oneVals.get(1));

        Assert.assertEquals("incorrect value", "C",twoVals.get(0));
        Assert.assertEquals("incorrect value", "D", twoVals.get(1));

        Assert.assertEquals("incorrect value", "E", threeVals.get(0));
        Assert.assertEquals("incorrect value", "F", threeVals.get(1));

        Assert.assertEquals("incorrect value", "G", fourVals.get(0));
        Assert.assertEquals("incorrect value", "H", fourVals.get(1));

        Assert.assertEquals("incorrect value", "I", fiveVals.get(0));
        Assert.assertEquals("incorrect value", "J", fiveVals.get(1));
        Assert.assertEquals("incorrect value", "K", fiveVals.get(2));



        //pointers are correct
        Assert.assertTrue("incorrect pointer", onePointers.get(0).getPage() == 8 && onePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", onePointers.get(1).getPage() == 8 && onePointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", twoPointers.get(0).getPage() == 8 && twoPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", twoPointers.get(1).getPage() == 9 && twoPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", threePointers.get(0).getPage() == 9 && threePointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", threePointers.get(1).getPage() == 9 && threePointers.get(1).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", fourPointers.get(0).getPage() == 10 && fourPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fourPointers.get(1).getPage() == 10 && fourPointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", fivePointers.get(0).getPage() == 10 && fivePointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", fivePointers.get(1).getPage() == 11 && fivePointers.get(1).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fivePointers.get(2).getPage() == 11 && fivePointers.get(2).getOffset() == 1);
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
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, left.getSize());
        Assert.assertEquals("incorrect node size", 2, leftVals.size());
        Assert.assertEquals("incorrect node size", 3, leftPointers.size());

        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 2, rightPointers.size());

        Assert.assertEquals("incorrect node size", 2, one.getSize());
        Assert.assertEquals("incorrect node size", 2, oneVals.size());
        Assert.assertEquals("incorrect node size", 2, onePointers.size());

        Assert.assertEquals("incorrect node size", 2, two.getSize());
        Assert.assertEquals("incorrect node size", 2, twoVals.size());
        Assert.assertEquals("incorrect node size", 2, twoPointers.size());

        Assert.assertEquals("incorrect node size", 2, three.getSize());
        Assert.assertEquals("incorrect node size", 2, threeVals.size());
        Assert.assertEquals("incorrect node size", 2, threePointers.size());

        Assert.assertEquals("incorrect node size", 2, four.getSize());
        Assert.assertEquals("incorrect node size", 2, fourVals.size());
        Assert.assertEquals("incorrect node size", 2, fourPointers.size());

        Assert.assertEquals("incorrect node size", 3, five.getSize());
        Assert.assertEquals("incorrect node size", 3, fiveVals.size());
        Assert.assertEquals("incorrect node size", 3, fivePointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", one.getNext(), leftPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", two.getNext(), leftPointers.get(2));
        Assert.assertEquals("leaves are linked incorrectly", three.getNext(), rightPointers.get(0));
        Assert.assertEquals("leaves are linked incorrectly", four.getNext(), rightPointers.get(1));
        Assert.assertNull(five.getNext());


        //values are correct
        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-13"),rootVals.get(0));

        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-09"), leftVals.get(0));
        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-11"),leftVals.get(1));

        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-16"), rightVals.get(0));

        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-01"),oneVals.get(0));
        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-04"), oneVals.get(1));

        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-09"),twoVals.get(0));
        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-10"), twoVals.get(1));

        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-11"), threeVals.get(0));
        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-12"), threeVals.get(1));

        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-13"), fourVals.get(0));
        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-15"), fourVals.get(1));

        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-16"), fiveVals.get(0));
        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-20"), fiveVals.get(1));
        Assert.assertEquals("incorrect value", new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-25"), fiveVals.get(2));



        //pointers are correct
        Assert.assertTrue("incorrect pointer", onePointers.get(0).getPage() == 8 && onePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", onePointers.get(1).getPage() == 8 && onePointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", twoPointers.get(0).getPage() == 8 && twoPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", twoPointers.get(1).getPage() == 9 && twoPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", threePointers.get(0).getPage() == 9 && threePointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", threePointers.get(1).getPage() == 9 && threePointers.get(1).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", fourPointers.get(0).getPage() == 10 && fourPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fourPointers.get(1).getPage() == 10 && fourPointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", fivePointers.get(0).getPage() == 10 && fivePointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", fivePointers.get(1).getPage() == 11 && fivePointers.get(1).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fivePointers.get(2).getPage() == 11 && fivePointers.get(2).getOffset() == 1);
    }

    @Test
    public void U_testBoolean(){
        BPlusTree<Boolean> treeBoolean = Utilities.deserializeBPT("TreeTesting_Boolean");

        BPTExternal<Boolean> root = (BPTExternal<Boolean>) treeBoolean.getRoot();
        ArrayList<Boolean> rootVals = root.getValues();
        ArrayList<BPointer> rootPointers = root.getPointers();

        //correct sizes
        Assert.assertEquals("incorrect node size", 2, root.getSize());
        Assert.assertEquals("incorrect node size", 2, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        //nodes are linked correctly
        Assert.assertNull(root.getNext());

        //correct values
        Assert.assertEquals("incorrect value", false,rootVals.get(0));
        Assert.assertEquals("incorrect value", true,rootVals.get(1));

        //pointers are correct
        Assert.assertTrue("incorrect pointer", rootPointers.get(0).getPage() == 8 && rootPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", rootPointers.get(1).getPage() == 9 && rootPointers.get(1).getOffset() == 2);

        String path = "data//overflow_Pages//overflow_TreeTesting_Boolean_false_0.class";
        Assert.assertTrue(new File(path).isFile()); //first page

        overflowPage[] falsePages = new overflowPage[2];

        falsePages[0] = Utilities.deserializeOverflow("TreeTesting_Boolean_false_0");
        Assert.assertEquals("overflow pages are not linked correctly", "TreeTesting_Boolean_false_1", falsePages[0].getNext());
        Assert.assertNull("overflow pages are not linked correctly", falsePages[0].getPrev());

        falsePages[1] = Utilities.deserializeOverflow("TreeTesting_Boolean_false_1");
        Assert.assertNull("overflow pages are not linked correctly", falsePages[1].getNext());
        Assert.assertEquals("overflow pages are not linked correctly", "TreeTesting_Boolean_false_0", falsePages[1].getPrev());

        //sizes are correct
        Assert.assertEquals("incorrect overflow page size", 3,falsePages[0].size());
        Assert.assertEquals("incorrect overflow page size", 1,falsePages[1].size());

        //pointers are correct
        Queue<Pointer> p =  falsePages[0].getPointers();
        Assert.assertTrue("incorrect pointer", ((BPointer) p.peek()).getPage() == 8 && ((BPointer) p.poll()).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", ((BPointer) p.peek()).getPage() == 8 && ((BPointer) p.poll()).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", ((BPointer) p.peek()).getPage() == 9 && ((BPointer) p.poll()).getOffset() == 0);

        p =  falsePages[1].getPointers();
        Assert.assertTrue("incorrect pointer", ((BPointer) p.peek()).getPage() == 9 && ((BPointer) p.poll()).getOffset() == 1);

        overflowPage[] truePages = new overflowPage[2];

        truePages[0] = Utilities.deserializeOverflow("TreeTesting_Boolean_true_0");
        Assert.assertEquals("overflow pages are not linked correctly", "TreeTesting_Boolean_true_1", truePages[0].getNext());
        Assert.assertNull("overflow pages are not linked correctly", truePages[0].getPrev());

        truePages[1] = Utilities.deserializeOverflow("TreeTesting_Boolean_true_1");
        Assert.assertNull("overflow pages are not linked correctly", truePages[1].getNext());
        Assert.assertEquals("overflow pages are not linked correctly", "TreeTesting_Boolean_true_0", truePages[1].getPrev());

        //sizes are correct
        Assert.assertEquals("incorrect overflow page size", 3,truePages[0].size());
        Assert.assertEquals("incorrect overflow page size", 2,truePages[1].size());

        //pointers are correct
        p =  truePages[0].getPointers();
        Assert.assertTrue("incorrect pointer", ((BPointer) p.peek()).getPage() == 10 && ((BPointer) p.poll()).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", ((BPointer) p.peek()).getPage() == 10 && ((BPointer) p.poll()).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", ((BPointer) p.peek()).getPage() == 10 && ((BPointer) p.poll()).getOffset() == 2);

        p =  truePages[1].getPointers();
        Assert.assertTrue("incorrect pointer", ((BPointer) p.peek()).getPage() == 11 && ((BPointer) p.poll()).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", ((BPointer) p.peek()).getPage() == 11 && ((BPointer) p.poll()).getOffset() == 1);

    }

    @Test
    public void V_testPolygon(){
        RTree treePoly = Utilities.deserializeRTree("TreeTesting_Polygon");
        RInternal root = (RInternal) treePoly.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        RInternal  left = (RInternal) Utilities.deserializeRNode(rootPointers.get(0));
        RInternal  right = (RInternal) Utilities.deserializeRNode(rootPointers.get(1));

        ArrayList<myPolygon> rootVals = root.getValues();
        ArrayList<myPolygon> leftVals = left.getValues();
        ArrayList<myPolygon> rightVals = right.getValues();

        ArrayList<String> leftPointers = left.getPointers();
        ArrayList<String> rightPointers = right.getPointers();

        RExternal one = (RExternal) Utilities.deserializeRNode(leftPointers.get(0));
        RExternal  two = (RExternal) Utilities.deserializeRNode(leftPointers.get(1));
        RExternal  three = (RExternal) Utilities.deserializeRNode(leftPointers.get(2));

        RExternal  four = (RExternal) Utilities.deserializeRNode(rightPointers.get(0));
        RExternal  five = (RExternal) Utilities.deserializeRNode(rightPointers.get(1));

        ArrayList<myPolygon> oneVals = one.getValues();
        ArrayList<myPolygon> twoVals = two.getValues();
        ArrayList<myPolygon> threeVals = three.getValues();
        ArrayList<myPolygon> fourVals = four.getValues();
        ArrayList<myPolygon> fiveVals = five.getValues();

        ArrayList<BPointer> onePointers = one.getPointers();
        ArrayList<BPointer> twoPointers = two.getPointers();
        ArrayList<BPointer> threePointers = three.getPointers();
        ArrayList<BPointer> fourPointers = four.getPointers();
        ArrayList<BPointer> fivePointers = five.getPointers();


        //correct sizes
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        Assert.assertEquals("incorrect node size", 2, left.getSize());
        Assert.assertEquals("incorrect node size", 2, leftVals.size());
        Assert.assertEquals("incorrect node size", 3, leftPointers.size());

        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 2, rightPointers.size());

        Assert.assertEquals("incorrect node size", 2, one.getSize());
        Assert.assertEquals("incorrect node size", 2, oneVals.size());
        Assert.assertEquals("incorrect node size", 2, onePointers.size());

        Assert.assertEquals("incorrect node size", 2, two.getSize());
        Assert.assertEquals("incorrect node size", 2, twoVals.size());
        Assert.assertEquals("incorrect node size", 2, twoPointers.size());

        Assert.assertEquals("incorrect node size", 2, three.getSize());
        Assert.assertEquals("incorrect node size", 2, threeVals.size());
        Assert.assertEquals("incorrect node size", 2, threePointers.size());

        Assert.assertEquals("incorrect node size", 2, four.getSize());
        Assert.assertEquals("incorrect node size", 2, fourVals.size());
        Assert.assertEquals("incorrect node size", 2, fourPointers.size());

        Assert.assertEquals("incorrect node size", 3, five.getSize());
        Assert.assertEquals("incorrect node size", 3, fiveVals.size());
        Assert.assertEquals("incorrect node size", 3, fivePointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", one.getNext(), leftPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", two.getNext(), leftPointers.get(2));
        Assert.assertEquals("leaves are linked incorrectly", three.getNext(), rightPointers.get(0));
        Assert.assertEquals("leaves are linked incorrectly", four.getNext(), rightPointers.get(1));
        Assert.assertNull(five.getNext());


        //values are correct
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(13),rootVals.get(0)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(9),leftVals.get(0)));
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(11),leftVals.get(1)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(16),rightVals.get(0)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(1),oneVals.get(0)));
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(4),oneVals.get(1)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(9),twoVals.get(0)));
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(10),twoVals.get(1)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(11),threeVals.get(0)));
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(12),threeVals.get(1)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(13),fourVals.get(0)));
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(15),fourVals.get(1)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(16),fiveVals.get(0)));
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(20),fiveVals.get(1)));
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(25),fiveVals.get(2)));



        //pointers are correct
        Assert.assertTrue("incorrect pointer", onePointers.get(0).getPage() == 8 && onePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", onePointers.get(1).getPage() == 8 && onePointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", twoPointers.get(0).getPage() == 8 && twoPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", twoPointers.get(1).getPage() == 9 && twoPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", threePointers.get(0).getPage() == 9 && threePointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", threePointers.get(1).getPage() == 9 && threePointers.get(1).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", fourPointers.get(0).getPage() == 10 && fourPointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fourPointers.get(1).getPage() == 10 && fourPointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", fivePointers.get(0).getPage() == 10 && fivePointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", fivePointers.get(1).getPage() == 11 && fivePointers.get(1).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", fivePointers.get(2).getPage() == 11 && fivePointers.get(2).getOffset() == 1);
    }

    @Test
    public void W_testPolygonDelete13_15_16_20() throws DBAppException {
        Hashtable<String, Object> tuple = new Hashtable<>();
        tuple.put("Integer",13);
        DB.deleteFromTable("TreeTesting", tuple);

        tuple.put("Integer",15);
        DB.deleteFromTable("TreeTesting", tuple);

        tuple.put("Integer",16);
        DB.deleteFromTable("TreeTesting", tuple);

        tuple.put("Integer",20);
        DB.deleteFromTable("TreeTesting", tuple);

        //----------------------------------TREE_STRUCTURE--------------------------------------

        RTree treePoly = Utilities.deserializeRTree("TreeTesting_Polygon");
        RInternal root = (RInternal) treePoly.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        RInternal  left = (RInternal) Utilities.deserializeRNode(rootPointers.get(0));
        RInternal  right = (RInternal) Utilities.deserializeRNode(rootPointers.get(1));

        ArrayList<myPolygon> rootVals = root.getValues();
        ArrayList<myPolygon> leftVals = left.getValues();
        ArrayList<myPolygon> rightVals = right.getValues();

        ArrayList<String> leftPointers = left.getPointers();
        ArrayList<String> rightPointers = right.getPointers();

        RExternal one = (RExternal) Utilities.deserializeRNode(leftPointers.get(0));
        RExternal  two = (RExternal) Utilities.deserializeRNode(leftPointers.get(1));

        RExternal  three = (RExternal) Utilities.deserializeRNode(rightPointers.get(0));
        RExternal  four = (RExternal) Utilities.deserializeRNode(rightPointers.get(1));

        ArrayList<myPolygon> oneVals = one.getValues();
        ArrayList<myPolygon> twoVals = two.getValues();
        ArrayList<myPolygon> threeVals = three.getValues();
        ArrayList<myPolygon> fourVals = four.getValues();

        ArrayList<BPointer> onePointers = one.getPointers();
        ArrayList<BPointer> twoPointers = two.getPointers();
        ArrayList<BPointer> threePointers = three.getPointers();
        ArrayList<BPointer> fourPointers = four.getPointers();


        //correct sizes
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        Assert.assertEquals("incorrect node size", 1, left.getSize());
        Assert.assertEquals("incorrect node size", 1, leftVals.size());
        Assert.assertEquals("incorrect node size", 2, leftPointers.size());

        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 2, rightPointers.size());

        Assert.assertEquals("incorrect node size", 2, one.getSize());
        Assert.assertEquals("incorrect node size", 2, oneVals.size());
        Assert.assertEquals("incorrect node size", 2, onePointers.size());

        Assert.assertEquals("incorrect node size", 2, two.getSize());
        Assert.assertEquals("incorrect node size", 2, twoVals.size());
        Assert.assertEquals("incorrect node size", 2, twoPointers.size());

        Assert.assertEquals("incorrect node size", 2, three.getSize());
        Assert.assertEquals("incorrect node size", 2, threeVals.size());
        Assert.assertEquals("incorrect node size", 2, threePointers.size());

        Assert.assertEquals("incorrect node size", 1, four.getSize());
        Assert.assertEquals("incorrect node size", 1, fourVals.size());
        Assert.assertEquals("incorrect node size", 1, fourPointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", one.getNext(), leftPointers.get(1));
        Assert.assertEquals("leaves are linked incorrectly", two.getNext(), rightPointers.get(0));
        Assert.assertEquals("leaves are linked incorrectly", three.getNext(), rightPointers.get(1));
        Assert.assertNull(four.getNext());


        //values are correct
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(11),rootVals.get(0)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(9),leftVals.get(0)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(25),rightVals.get(0)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(1),oneVals.get(0)));
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(4),oneVals.get(1)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(9),twoVals.get(0)));
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(10),twoVals.get(1)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(11),threeVals.get(0)));
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(12),threeVals.get(1)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(25),fourVals.get(0)));

        //pointers are correct
        Assert.assertTrue("incorrect pointer", onePointers.get(0).getPage() == 8 && onePointers.get(0).getOffset() == 0);
        Assert.assertTrue("incorrect pointer", onePointers.get(1).getPage() == 8 && onePointers.get(1).getOffset() == 1);

        Assert.assertTrue("incorrect pointer", twoPointers.get(0).getPage() == 8 && twoPointers.get(0).getOffset() == 2);
        Assert.assertTrue("incorrect pointer", twoPointers.get(1).getPage() == 9 && twoPointers.get(1).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", threePointers.get(0).getPage() == 9 && threePointers.get(0).getOffset() == 1);
        Assert.assertTrue("incorrect pointer", threePointers.get(1).getPage() == 9 && threePointers.get(1).getOffset() == 2);

        Assert.assertTrue("incorrect pointer", fourPointers.get(0).getPage() == 11 && fourPointers.get(0).getOffset() == 0);

    }

    @Test
    public void X_testPolygonDelete11_12_10_25_9() throws DBAppException {
        Hashtable<String, Object> tuple = new Hashtable<>();
        tuple.put("Integer",11);
        DB.deleteFromTable("TreeTesting", tuple);

        tuple.put("Integer",12);
        DB.deleteFromTable("TreeTesting", tuple);

        tuple.put("Integer",10);
        DB.deleteFromTable("TreeTesting", tuple);

        tuple.put("Integer",25);
        DB.deleteFromTable("TreeTesting", tuple);

        tuple.put("Integer",9);
        DB.deleteFromTable("TreeTesting", tuple);

        //----------------------------------TREE_STRUCTURE--------------------------------------

        RTree treePoly = Utilities.deserializeRTree("TreeTesting_Polygon");
        RInternal root = (RInternal) treePoly.getRoot();
        ArrayList<String> rootPointers = root.getPointers();

        RExternal  left = (RExternal) Utilities.deserializeRNode(rootPointers.get(0));
        RExternal  right = (RExternal) Utilities.deserializeRNode(rootPointers.get(1));

        ArrayList<myPolygon> rootVals = root.getValues();
        ArrayList<myPolygon> leftVals = left.getValues();
        ArrayList<myPolygon> rightVals = right.getValues();

        ArrayList<BPointer> leftPointers = left.getPointers();
        ArrayList<BPointer> rightPointers = right.getPointers();

        //correct sizes
        Assert.assertEquals("incorrect node size", 1, root.getSize());
        Assert.assertEquals("incorrect node size", 1, rootVals.size());
        Assert.assertEquals("incorrect node size", 2, rootPointers.size());

        Assert.assertEquals("incorrect node size", 1, left.getSize());
        Assert.assertEquals("incorrect node size", 1, leftVals.size());
        Assert.assertEquals("incorrect node size", 1, leftPointers.size());


        Assert.assertEquals("incorrect node size", 1, right.getSize());
        Assert.assertEquals("incorrect node size", 1, rightVals.size());
        Assert.assertEquals("incorrect node size", 1, rightPointers.size());


        //nodes are linked correctly
        Assert.assertEquals("leaves are linked incorrectly", left.getNext(), rootPointers.get(1));
        Assert.assertNull("leaves are linked incorrectly", right.getNext());


        //values are correct
        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(4),rootVals.get(0)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(1),leftVals.get(0)));

        Assert.assertTrue("incorrect value", Utilities.polygonsEqual(square(4),rightVals.get(0)));

        //pointers are correct
        Assert.assertTrue("incorrect pointer", leftPointers.get(0).getPage() == 8 && leftPointers.get(0).getOffset() == 0);

        Assert.assertTrue("incorrect pointer", rightPointers.get(0).getPage() == 8 && rightPointers.get(0).getOffset() == 1);

    }

    @Test
    public void Y_testPolygonTreeEmpty() throws DBAppException {
        Hashtable<String, Object> tuple = new Hashtable<>();
        tuple.put("Integer",4);
        DB.deleteFromTable("TreeTesting", tuple);

        tuple.put("Integer",1);
        DB.deleteFromTable("TreeTesting", tuple);

        //----------------------------------TREE_STRUCTURE--------------------------------------

        RTree treePoly = Utilities.deserializeRTree("TreeTesting_Polygon");
        Assert.assertNull("tree root should be null", treePoly.getRoot());
    }

    @Test
    public void Z_deleteAllOverflow(){
        File dir = new File("data//overflow_Pages");

        for(File file: dir.listFiles()) //clear pages from before
            if (!file.isDirectory())
                file.delete();

        //create some pages
        for (int i = 0; i < 250; i++) overflowPage.insert("dummy",new BPointer(i/3,i%3));

        //then delete them all
        overflowPage.destroyAllPages("dummy");

        Assert.assertEquals("failed to destroy all pages", 0,dir.listFiles().length);

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

        dir = new File("data//R//Trees"); //delete BPlus trees

        for(File file: dir.listFiles())
            if (!file.isDirectory())
                file.delete();

        dir = new File("data//R//R_Nodes"); //delete all BPlus tree nodes

        for(File file: dir.listFiles())
            if (!file.isDirectory())
                file.delete();


        dir = new File("data//overflow_Pages//.gitkeep");
        dir.createNewFile();
        dir = new File("data//BPlus//B+_Nodes//.gitkeep");
        dir.createNewFile();
        dir = new File("data//BPlus//Trees//.gitkeep");
        dir.createNewFile();
        dir = new File("data//R//R_Nodes//.gitkeep");
        dir.createNewFile();
        dir = new File("data//R//Trees//.gitkeep");
        dir.createNewFile();

    }

    @Ignore
    public static myPolygon square(int side){
        Polygon temp = new Polygon(); //square with side length 1
        temp.addPoint(0,0);temp.addPoint(side,0);temp.addPoint(0,side);
        temp.addPoint(side,side);

        return new myPolygon(temp);
    }


}