package DatabaseEngine.testing;


import DatabaseEngine.*;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.awt.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class selectTest {
    private static DBApp DB;
    @BeforeClass
    public static void prepareTables() throws DBAppException, ParseException {
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
            DB.createTable("TreeTesting1","Integer", table);
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }

        try {
            DB.createTable("TreeTesting2","Polygon", table);
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }

        insertAll(1);
        insertAll(2);
    }

    @Test
    //all other data types except polygon should work the same way as an integer according to their compareTo() method
    public void A_clusteringKey() throws DBAppException {
        //VALUES: 1, 4, 9, 10, 11, 12, 13, 15, 16, 20, 25
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[1];
        arrSQLTerms[0] = new SQLTerm();
		arrSQLTerms[0]._strTableName = "TreeTesting1";
		arrSQLTerms[0]._strColumnName= "Integer";
		arrSQLTerms[0]._strOperator = "<";
		arrSQLTerms[0]._objValue = 13;

        BSet<Integer> lt = new BSet<>();
        Iterator temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) lt.add((Integer) ((Vector) temp.next()).get(1));

        arrSQLTerms[0]._strOperator = "<=";

        BSet<Integer> lte = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) lte.add((Integer) ((Vector) temp.next()).get(1));

        arrSQLTerms[0]._strOperator = "=";

        BSet<Integer> eq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) eq.add((Integer) ((Vector) temp.next()).get(1));

        arrSQLTerms[0]._strOperator = "!=";

        BSet<Integer> neq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) neq.add((Integer) ((Vector) temp.next()).get(1));

        arrSQLTerms[0]._strOperator = ">";

        BSet<Integer> gt = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) gt.add((Integer) ((Vector) temp.next()).get(1));

        arrSQLTerms[0]._strOperator = ">=";

        BSet<Integer> gte = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) gte.add((Integer) ((Vector) temp.next()).get(1));

        //less than
        Assert.assertEquals("incorrect number of result elements",6,lt.size());
        Assert.assertTrue("missing result",lt.contains(1));
        Assert.assertTrue("missing result",lt.contains(4));
        Assert.assertTrue("missing result",lt.contains(9));
        Assert.assertTrue("missing result",lt.contains(10));
        Assert.assertTrue("missing result",lt.contains(11));
        Assert.assertTrue("missing result",lt.contains(12));

        //less than or equal
        Assert.assertEquals("incorrect number of result elements",7,lte.size());
        lte = lte.EXCEPT(lt);
        Assert.assertEquals("incorrect number of result elements",1,lte.size());
        Assert.assertTrue("missing result",lte.contains(13));

        //equal
        Assert.assertEquals("incorrect number of result elements",1,eq.size());
        Assert.assertTrue("missing result",eq.contains(13));

        //not equal
        Assert.assertEquals("incorrect number of result elements",10,neq.size());
        Assert.assertEquals("incorrect number of result elements",0,neq.AND(eq).size());

        //greater than
        Assert.assertEquals("incorrect number of result elements",4,gt.size());
        Assert.assertTrue("missing result",gt.contains(15));
        Assert.assertTrue("missing result",gt.contains(16));
        Assert.assertTrue("missing result",gt.contains(20));
        Assert.assertTrue("missing result",gt.contains(25));

        //greater than or equal
        Assert.assertEquals("incorrect number of result elements",5,gte.size());
        gte = gte.EXCEPT(gt);
        Assert.assertEquals("incorrect number of result elements",1,gte.size());
        Assert.assertTrue("missing result",gte.contains(13));

    }


    @Test
    public void B_nonClusteringKey() throws DBAppException, ParseException {
        //VALUES: A, B, C, D, E, F, G, H, I, J, K
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[1];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "TreeTesting1";
        arrSQLTerms[0]._strColumnName= "String";
        arrSQLTerms[0]._strOperator = "<";
        arrSQLTerms[0]._objValue = "G";

        BSet<String> lt = new BSet<>();
        Iterator temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) lt.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = "<=";

        BSet<String> lte = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) lte.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = "=";

        BSet<String> eq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) eq.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = "!=";

        BSet<String> neq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) neq.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = ">";

        BSet<String> gt = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) gt.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = ">=";

        BSet<String> gte = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) gte.add((String) ((Vector) temp.next()).get(3));

        //less than
        Assert.assertEquals("incorrect number of result elements",6,lt.size());
        Assert.assertTrue("missing result",lt.contains("A"));
        Assert.assertTrue("missing result",lt.contains("B"));
        Assert.assertTrue("missing result",lt.contains("C"));
        Assert.assertTrue("missing result",lt.contains("D"));
        Assert.assertTrue("missing result",lt.contains("E"));
        Assert.assertTrue("missing result",lt.contains("F"));

        //less than or equal
        Assert.assertEquals("incorrect number of result elements",7,lte.size());
        lte = lte.EXCEPT(lt);
        Assert.assertEquals("incorrect number of result elements",1,lte.size());
        Assert.assertTrue("missing result",lte.contains("G"));

        //equal
        Assert.assertEquals("incorrect number of result elements",1,eq.size());
        Assert.assertTrue("missing result",eq.contains("G"));

        //not equal
        Assert.assertEquals("incorrect number of result elements",10,neq.size());
        Assert.assertEquals("incorrect number of result elements",0,neq.AND(eq).size());

        //greater than
        Assert.assertEquals("incorrect number of result elements",4,gt.size());
        Assert.assertTrue("missing result",gt.contains("H"));
        Assert.assertTrue("missing result",gt.contains("I"));
        Assert.assertTrue("missing result",gt.contains("J"));
        Assert.assertTrue("missing result",gt.contains("K"));

        //greater than or equal
        Assert.assertEquals("incorrect number of result elements",5,gte.size());
        gte = gte.EXCEPT(gt);
        Assert.assertEquals("incorrect number of result elements",1,gte.size());
        Assert.assertTrue("missing result",gte.contains("G"));

    }

    @Test
    public void C_indexed() throws DBAppException {
        DB.createBTreeIndex("TreeTesting1","String");

        //VALUES: A, B, C, D, E, F, G, H, I, J, K
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[1];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "TreeTesting1";
        arrSQLTerms[0]._strColumnName= "String";
        arrSQLTerms[0]._strOperator = "<";
        arrSQLTerms[0]._objValue = "G";

        BSet<String> lt = new BSet<>();
        Iterator temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) lt.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = "<=";

        BSet<String> lte = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) lte.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = "=";

        BSet<String> eq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) eq.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = "!=";

        BSet<String> neq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) neq.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = ">";

        BSet<String> gt = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) gt.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = ">=";

        BSet<String> gte = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) gte.add((String) ((Vector) temp.next()).get(3));

        //less than
        Assert.assertEquals("incorrect number of result elements",6,lt.size());
        Assert.assertTrue("missing result",lt.contains("A"));
        Assert.assertTrue("missing result",lt.contains("B"));
        Assert.assertTrue("missing result",lt.contains("C"));
        Assert.assertTrue("missing result",lt.contains("D"));
        Assert.assertTrue("missing result",lt.contains("E"));
        Assert.assertTrue("missing result",lt.contains("F"));

        //less than or equal
        Assert.assertEquals("incorrect number of result elements",7,lte.size());
        lte = lte.EXCEPT(lt);
        Assert.assertEquals("incorrect number of result elements",1,lte.size());
        Assert.assertTrue("missing result",lte.contains("G"));

        //equal
        Assert.assertEquals("incorrect number of result elements",1,eq.size());
        Assert.assertTrue("missing result",eq.contains("G"));

        //not equal
        Assert.assertEquals("incorrect number of result elements",10,neq.size());
        Assert.assertEquals("incorrect number of result elements",0,neq.AND(eq).size());

        //greater than
        Assert.assertEquals("incorrect number of result elements",4,gt.size());
        Assert.assertTrue("missing result",gt.contains("H"));
        Assert.assertTrue("missing result",gt.contains("I"));
        Assert.assertTrue("missing result",gt.contains("J"));
        Assert.assertTrue("missing result",gt.contains("K"));

        //greater than or equal
        Assert.assertEquals("incorrect number of result elements",5,gte.size());
        gte = gte.EXCEPT(gt);
        Assert.assertEquals("incorrect number of result elements",1,gte.size());
        Assert.assertTrue("missing result",gte.contains("G"));

    }

    @Test
    public void D_indexedWDups() throws DBAppException, ParseException {
        Hashtable<String, Object> tuple = new Hashtable<String, Object>();
        tuple.put("Integer",4);
        tuple.put("Double",4.6);
        tuple.put("String", "G");
        tuple.put("Date",new SimpleDateFormat("yyyy-MM-dd").parse("2020-03-04"));
        tuple.put("Boolean",false);

        Polygon tempP = new Polygon(); //square with side length 1
        tempP.addPoint(0,0);tempP.addPoint(2,0);tempP.addPoint(0,8);
        tempP.addPoint(2,8);
        tuple.put("Polygon",new myPolygon(tempP));

        DB.insertIntoTable("TreeTesting1",tuple);
        DB.insertIntoTable("TreeTesting2",tuple);

        tempP = new Polygon(); //square with side length 1
        tempP.addPoint(0,0);tempP.addPoint(1,0);tempP.addPoint(0,16);
        tempP.addPoint(1,16);
        tuple.put("Polygon",new myPolygon(tempP));

        DB.insertIntoTable("TreeTesting1",tuple);
        DB.insertIntoTable("TreeTesting2",tuple);

        tempP = new Polygon(); //square with side length 1
        tempP.addPoint(0,0);tempP.addPoint(-4,0);tempP.addPoint(0,-4);
        tempP.addPoint(-4,-4);
        tuple.put("Polygon",new myPolygon(tempP));

        DB.insertIntoTable("TreeTesting1",tuple);
        DB.insertIntoTable("TreeTesting2",tuple);

        tempP = new Polygon(); //square with side length 1
        tempP.addPoint(0,0);tempP.addPoint(8,0);tempP.addPoint(0,2);
        tempP.addPoint(8,2);
        tuple.put("Polygon",new myPolygon(tempP));

        DB.insertIntoTable("TreeTesting1",tuple);
        DB.insertIntoTable("TreeTesting2",tuple);

        //===============================TESTS==============================

        //VALUES: A, B, B, B, B, B, C, D, E, F, G, H, I, J, K
        int size = 0;

        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[1];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "TreeTesting1";
        arrSQLTerms[0]._strColumnName= "String";
        arrSQLTerms[0]._strOperator = "<";
        arrSQLTerms[0]._objValue = "G";

        BSet<String> lt = new BSet<>();
        Iterator temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) lt.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = "<=";

        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){ temp.next();size++;}
        Assert.assertEquals("incorrect number of result elements",11,size);


        arrSQLTerms[0]._strOperator = "=";

        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        size = 0;
        while (temp.hasNext()){ temp.next();size++;}
        Assert.assertEquals("incorrect number of result elements",5,size);

        arrSQLTerms[0]._strOperator = "!=";

        BSet<String> neq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) neq.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = ">";

        BSet<String> gt = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()) gt.add((String) ((Vector) temp.next()).get(3));

        arrSQLTerms[0]._strOperator = ">=";

        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        size = 0;
        while (temp.hasNext()){ temp.next();size++;}
        Assert.assertEquals("incorrect number of result elements",9,size);

        //less than
        Assert.assertEquals("incorrect number of result elements",6,lt.size());
        Assert.assertTrue("missing result",lt.contains("A"));
        Assert.assertTrue("missing result",lt.contains("B"));
        Assert.assertTrue("missing result",lt.contains("C"));
        Assert.assertTrue("missing result",lt.contains("D"));
        Assert.assertTrue("missing result",lt.contains("E"));
        Assert.assertTrue("missing result",lt.contains("F"));

        //not equal
        Assert.assertEquals("incorrect number of result elements",10,neq.size());

        //greater than
        Assert.assertEquals("incorrect number of result elements",4,gt.size());
        Assert.assertTrue("missing result",gt.contains("H"));
        Assert.assertTrue("missing result",gt.contains("I"));
        Assert.assertTrue("missing result",gt.contains("J"));
        Assert.assertTrue("missing result",gt.contains("K"));
    }

    @Test
    public void E_PolygonClusteringKey() throws DBAppException {

        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[1];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "TreeTesting2";
        arrSQLTerms[0]._strColumnName= "Polygon";
        arrSQLTerms[0]._strOperator = "<";
        arrSQLTerms[0]._objValue = square(4);

        int size = 0;

        BSet<Integer> lt = new BSet<>();
        Iterator temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            lt.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",1,size);
        size = 0;

        arrSQLTerms[0]._strOperator = "<=";

        BSet<Integer> lte = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            lte.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",6,size);
        size = 0;

        arrSQLTerms[0]._strOperator = "=";

        BSet<Integer> eq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            eq.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",1,size);
        size = 0;

        arrSQLTerms[0]._strOperator = "!=";

        BSet<Integer> neq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            neq.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",14,size);
        size = 0;

        arrSQLTerms[0]._strOperator = ">";

        BSet<Integer> gt = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            gt.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",9,size);
        size = 0;

        arrSQLTerms[0]._strOperator = ">=";

        BSet<Integer> gte = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            gte.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",14,size);

        //less than
        Assert.assertEquals("incorrect number of result elements",1,lt.size());
        Assert.assertTrue("missing result",lt.contains(1));

        //less than or equal
        Assert.assertEquals("incorrect number of result elements",2,lte.size());
        Assert.assertTrue("missing result",lte.contains(4*4));
        Assert.assertTrue("missing result",lte.contains(1));

        //equal
        Assert.assertEquals("incorrect number of result elements",1,eq.size());
        Assert.assertTrue("missing result",eq.contains(4*4));

        //not equal
        Assert.assertEquals("incorrect number of result elements",11,neq.size());
        Assert.assertEquals("incorrect number of result elements",1,neq.AND(eq).size());

        //greater than
        Assert.assertEquals("incorrect number of result elements",9,gt.size());
        Assert.assertTrue("missing result",gt.contains(15*15));
        Assert.assertTrue("missing result",gt.contains(16*16));
        Assert.assertTrue("missing result",gt.contains(20*20));
        Assert.assertTrue("missing result",gt.contains(25*25));

        //greater than or equal
        Assert.assertEquals("incorrect number of result elements",10,gte.size());
        gte = gte.EXCEPT(gt);
        Assert.assertEquals("incorrect number of result elements",1,gte.size());
        Assert.assertTrue("missing result",gte.contains(4*4));


    }

    @Test
    public void F_PolygonNonClusteringKey() throws DBAppException {

        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[1];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "TreeTesting1";
        arrSQLTerms[0]._strColumnName= "Polygon";
        arrSQLTerms[0]._strOperator = "<";
        arrSQLTerms[0]._objValue = square(4);

        int size = 0;

        BSet<Integer> lt = new BSet<>();
        Iterator temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            lt.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",1,size);
        size = 0;

        arrSQLTerms[0]._strOperator = "<=";

        BSet<Integer> lte = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            lte.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",6,size);
        size = 0;

        arrSQLTerms[0]._strOperator = "=";

        BSet<Integer> eq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            eq.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",1,size);
        size = 0;

        arrSQLTerms[0]._strOperator = "!=";

        BSet<Integer> neq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            neq.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",14,size);
        size = 0;

        arrSQLTerms[0]._strOperator = ">";

        BSet<Integer> gt = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            gt.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",9,size);
        size = 0;

        arrSQLTerms[0]._strOperator = ">=";

        BSet<Integer> gte = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            gte.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",14,size);

        //less than
        Assert.assertEquals("incorrect number of result elements",1,lt.size());
        Assert.assertTrue("missing result",lt.contains(1));

        //less than or equal
        Assert.assertEquals("incorrect number of result elements",2,lte.size());
        Assert.assertTrue("missing result",lte.contains(4*4));
        Assert.assertTrue("missing result",lte.contains(1));

        //equal
        Assert.assertEquals("incorrect number of result elements",1,eq.size());
        Assert.assertTrue("missing result",eq.contains(4*4));

        //not equal
        Assert.assertEquals("incorrect number of result elements",11,neq.size());
        Assert.assertEquals("incorrect number of result elements",1,neq.AND(eq).size());

        //greater than
        Assert.assertEquals("incorrect number of result elements",9,gt.size());
        Assert.assertTrue("missing result",gt.contains(15*15));
        Assert.assertTrue("missing result",gt.contains(16*16));
        Assert.assertTrue("missing result",gt.contains(20*20));
        Assert.assertTrue("missing result",gt.contains(25*25));

        //greater than or equal
        Assert.assertEquals("incorrect number of result elements",10,gte.size());
        gte = gte.EXCEPT(gt);
        Assert.assertEquals("incorrect number of result elements",1,gte.size());
        Assert.assertTrue("missing result",gte.contains(4*4));

    }

    @Test
    public void G_PolygonIndexed() throws DBAppException {
        //SQUARE SIDES: 1, 4, 9, 10, 11, 12, 13, 15, 16, 20, 25
        DB.createRTreeIndex("TreeTesting2","Polygon");

        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[1];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "TreeTesting2";
        arrSQLTerms[0]._strColumnName= "Polygon";
        arrSQLTerms[0]._strOperator = "<";
        arrSQLTerms[0]._objValue = square(4);

        int size = 0;

        BSet<Integer> lt = new BSet<>();
        Iterator temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            lt.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",1,size);
        size = 0;

        arrSQLTerms[0]._strOperator = "<=";

        BSet<Integer> lte = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            lte.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",6,size);
        size = 0;

        arrSQLTerms[0]._strOperator = "=";

        BSet<Integer> eq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            eq.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",1,size);
        size = 0;

        arrSQLTerms[0]._strOperator = "!=";

        BSet<Integer> neq = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            neq.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",14,size);
        size = 0;

        arrSQLTerms[0]._strOperator = ">";

        BSet<Integer> gt = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            gt.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",9,size);
        size = 0;

        arrSQLTerms[0]._strOperator = ">=";

        BSet<Integer> gte = new BSet<>();
        temp = DB.selectFromTable(arrSQLTerms,new String[]{});
        while (temp.hasNext()){
            Dimension d = ((myPolygon) ((Vector) temp.next()).get(2)).getBounds().getSize();
            size++;
            gte.add(d.height*d.width);
        }
        Assert.assertEquals("wrong result set size",14,size);

        //less than
        Assert.assertEquals("incorrect number of result elements",1,lt.size());
        Assert.assertTrue("missing result",lt.contains(1));

        //less than or equal
        Assert.assertEquals("incorrect number of result elements",2,lte.size());
        Assert.assertTrue("missing result",lte.contains(4*4));
        Assert.assertTrue("missing result",lte.contains(1));

        //equal
        Assert.assertEquals("incorrect number of result elements",1,eq.size());
        Assert.assertTrue("missing result",eq.contains(4*4));

        //not equal
        Assert.assertEquals("incorrect number of result elements",11,neq.size());
        Assert.assertEquals("incorrect number of result elements",1,neq.AND(eq).size());

        //greater than
        Assert.assertEquals("incorrect number of result elements",9,gt.size());
        Assert.assertTrue("missing result",gt.contains(15*15));
        Assert.assertTrue("missing result",gt.contains(16*16));
        Assert.assertTrue("missing result",gt.contains(20*20));
        Assert.assertTrue("missing result",gt.contains(25*25));

        //greater than or equal
        Assert.assertEquals("incorrect number of result elements",10,gte.size());
        gte = gte.EXCEPT(gt);
        Assert.assertEquals("incorrect number of result elements",1,gte.size());
        Assert.assertTrue("missing result",gte.contains(4*4));

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

    @Ignore
    public static void initialScenario(int table) throws DBAppException, ParseException {
        insert(1,0.9,"A", "2020-03-01", false, 1,table);
        insert(25,25.9,"K", "2020-03-25", true, 25,table);
        insert(16,16.3,"I", "2020-03-16", true, 16,table);
        insert(9,9.8,"C", "2020-03-09", false, 9,table);
        insert(4,4.6,"B", "2020-03-04", false, 4,table);
    }

    @Ignore
    public static void insertAll(int table) throws DBAppException, ParseException {
        initialScenario(table);
        insert(20,20.1,"J", "2020-03-20", true, 20,table);
        insert(13,13.2,"G", "2020-03-13", true, 13,table);
        insert(15,15.7,"H", "2020-03-15", true, 15,table);
        insert(10,10.56,"D", "2020-03-10", false, 10,table);
        insert(11,11.23,"E", "2020-03-11", false, 11,table);
        insert(12,12.49,"F", "2020-03-12", true, 12,table);
    }

    @Ignore
    public static void insert(Object integer, Object Double,Object string, String date, Object Boolean, int squareSide,int table ) throws DBAppException, ParseException {
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

        DB.insertIntoTable("TreeTesting"+table,tuple);
    }

    @Ignore
    public static myPolygon square(int side){
        Polygon temp = new Polygon(); //square with side length 1
        temp.addPoint(0,0);temp.addPoint(side,0);temp.addPoint(0,side);
        temp.addPoint(side,side);

        return new myPolygon(temp);
    }


}