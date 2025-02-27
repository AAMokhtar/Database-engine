package DatabaseEngine; //change to team name before submitting

import java.awt.*;
import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBAppTest {
    static ArrayList<String> tables; //table names
    static Hashtable<String,String> clusteringKeys; //clustering key for each table
    static HashMap<String,Hashtable<String,String>> columns; //table name -> column name -> type
    static HashMap<String,ArrayList<Hashtable<String,Object>>> tableVals;//table name -> tuples
    static DBApp DB; //instance of the engine
    static String[] types = {"java.lang.Integer", "java.lang.String", "java.lang.Boolean"
            , "java.util.Date", "java.lang.Double", "java.awt.Polygon"}; //all possible data types

    public static void main(String[] args) throws DBAppException, ParseException { //run to generate tables and insert tuples into them
//        tables = new ArrayList<>();
//        columns = new HashMap<>();
//        tableVals = new HashMap<>();
//        clusteringKeys = new Hashtable<>();
//        
//    	DB = new DBApp(); //initialize the engine

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
        
         

//        createTable(5,true); //create a table with 5 columns
//        createTable(5,true);
//        DB.init();
//        Insert(tables.get(0),10); //insert 20000 records into it
//        Insert(tables.get(1),10);
        
          
		DB = new DBApp(); // initialize the engine
		DB.init();

		Hashtable<String, String> col = new Hashtable<>();

		col.put("bool", "java.lang.Boolean");
		col.put("int", "java.lang.Integer");
		col.put("str", "java.lang.String");
		col.put("date", "java.util.Date");
		col.put("dbl", "java.lang.Double");
		col.put("poly", "java.awt.Polygon");

		DB.createTable("test", "poly", col);

		DB.createBTreeIndex("test", "bool");
		DB.createBTreeIndex("test", "int");
		DB.createBTreeIndex("test", "str");
		DB.createBTreeIndex("test", "date");
		DB.createBTreeIndex("test", "dbl");
		DB.createRTreeIndex("test", "poly");

		Hashtable<String, Object> newHash = new Hashtable<String, Object>();

		for (int i = 0; i < 200; i++)
		{
			Hashtable<String, Object> h = new Hashtable<>();
			h.put("bool", randomBool());
			h.put("int", RandomInt(0, 10000000));
			h.put("str", randomString(20));
			h.put("date", RandomDate());
			h.put("dbl", randomDouble(0, 100));
			h.put("poly", randomPolygon(5));
			
			DB.insertIntoTable("test", h);
		}

		newHash.put("bool", false);
		newHash.put("int", 133337);
		newHash.put("str", "mimi");
		newHash.put("date", new SimpleDateFormat("YYYY-MM-DD").parse("2029-11-08"));
		newHash.put("dbl", 4.20);
		int[] x = {2,1,2,1};
		int[] y = {1,1,2,2};
		Polygon pol = new Polygon(x, y, 4);
		newHash.put("poly", pol);

		DB.insertIntoTable("test", newHash);
		DB.insertIntoTable("test", newHash);
		DB.insertIntoTable("test", newHash);
		DB.insertIntoTable("test", newHash);
		DB.insertIntoTable("test", newHash);
		DB.insertIntoTable("test", newHash);
		DB.insertIntoTable("test", newHash);
		DB.insertIntoTable("test", newHash);
		DB.insertIntoTable("test", newHash);
		DB.insertIntoTable("test", newHash);
		
		Table t = Utilities.deserializeTable("test");
		
		for(int n : t.getPages())
		{
			System.out.println("Page " + n + " : ");
			
			Page p = Utilities.deserializePage(n);
			
			for (Vector v : p.getPageElements())
				System.out.println(v);
			System.out.println("-------------------------------------------------");
			
			Utilities.serializePage(p);
		}

		Utilities.serializeTable(t);

		Hashtable<String, Object> hash = new Hashtable<String, Object>();

		hash.put("bool", true);
		hash.put("int", 222222222);
		hash.put("str", "bibo");
		hash.put("date", new Date());
		hash.put("dbl", 6.66);
		int[] xp = {1,5,1,5};
		int[] yp = {1,5,5,1};
		pol = new Polygon(xp, yp, 4);
		hash.put("poly", pol);

    	  
    	
    	DB.updateTable("test","(1,1),(2,2),(1,2),(2,1)", hash);

		System.out.println("-----------------------------------------------------------------------------");

		t = Utilities.deserializeTable("test");
		
		for(int n : t.getPages())
		{
			System.out.println("Page " + n + " : ");
			
			Page p = Utilities.deserializePage(n);
			
			for (Vector v : p.getPageElements())
				System.out.println(v);
			System.out.println("-------------------------------------------------");
			
			Utilities.serializePage(p);
		}

		Utilities.serializeTable(t);
    	  

//        deleteFromTable(tables.get(0),500); //delete 500 records
//        deleteFromTable(tables.get(1),500);
//        updateTable(tables.get(0), 500); //update 500 records
//        updateTable(tables.get(1), 500);

//        SQLTerm[] t = new SQLTerm[3];
//        t[0] = new SQLTerm();
//        t[1] = new SQLTerm();
//        t[2] = new SQLTerm();
//
//        t[0]._strTableName = "obevdjbiaoqgod";
//        t[0]._strColumnName = "uyjyhuxf";
//        t[0]._strOperator = ">";
//        t[0]._objValue = 10;
//
//        t[1]._strTableName = "obevdjbiaoqgod";
//        t[1]._strColumnName = "kskr";
//        t[1]._strOperator = "=";
//        t[1]._objValue = false;
//
//        t[2]._strTableName = "obevdjbiaoqgod";
//        t[2]._strColumnName = "nvhmexdhcb";
//        t[2]._strOperator = "<";
//        t[2]._objValue = "m";
//
//        String[] operators = {"AND","AND"};
//
//        Iterator res = DB.selectFromTable(t, operators);
//
//        while (res.hasNext()){
//            Vector cur = (Vector) res.next();
//            System.out.println(cur.get(2) + " " + cur.get(0) + " " + cur.get(3));
//        }
    }

    public static String randomString(int length) { //a string of random characters from a-z of the desired length
        String res = "";

        for (int i = 0; i < length; i++) { //append characters
            res += (char) (Math.random() * ('z' - 'a' + 1) + 'a');
        }

        return res;
    }

    public static void createTable(int colnums,boolean Bindex) throws DBAppException { //
        Hashtable<String,String> table = randomTableColumns(colnums); //make columns with random names and types
        String name = randomString(RandomInt(5, 15)); //random table name from 5-15 characters in length

        tables.add(name); //save table name
        columns.put(name,table); //save name,type pairs

        tableVals.put(name,new ArrayList<>()); //initializes an array for tuples
        String clusteringKey = pickKey(table);
        clusteringKeys.put(name,clusteringKey); //save table's clustering key
        DB.createTable(name, clusteringKey, table); //create a table with the previous info

        if (Bindex){ //create B+ indices for all suitable columns
            Set<String> col = table.keySet();
            for(String str: col){
                if (!table.get(str).equals("java.awt.Polygon"))
                    DB.createBTreeIndex(name,str);
            }
        }
    }

    public static void updateTable(String table, int updates) throws DBAppException, ParseException { //update the table a number of times

        for (int i = 0; i < updates; i++) {
            Hashtable<String, Object> newTuple = createTuple(table);
            String clusteringKeyVal = (String) getRandomClusteringKeyVal(table); //get random key value
            DB.updateTable(table,clusteringKeyVal,newTuple);  //update table with new tuple

            //update saved values
            ArrayList<Hashtable<String,Object>> tuples = tableVals.get(table);

            //saved data will be incorrect from now on
        }
    }

    public static void deleteFromTable(String table, int deletes) throws DBAppException { //deletes an number of tuples from a table
        for (int i = 0; i < deletes; i++) {
            DB.deleteFromTable(table,getRandomTuple(table)); //delete from table
        }

        //saved data will be incorrect from now on
    }

    public static Hashtable<String, Object> getRandomTuple(String table){
        ArrayList<Hashtable<String,Object>> tuples = tableVals.get(table);
        int index = RandomInt(0,tuples.size() - 1);
        return tuples.get(index);
    }

    public static Object getRandomClusteringKeyVal(String table){ //random clustering key value from a table
        ArrayList<Hashtable<String,Object>> tuples = tableVals.get(table);
        int index = RandomInt(0,tuples.size() - 1);
        return tuples.get(index).get(clusteringKeys.get(table));
    }


    public static Hashtable<String, String> randomTableColumns(int length) {
        Hashtable<String, String> res = new Hashtable<>(); //

        for (int i = 0; i < length; i++) { //populate hashtable
            res.put(randomString(RandomInt(3, 10)), randomDataType());
        }

        return res;
    }

    public static String randomDataType() { //returns one of the six allowed data types
        return types[RandomInt(0, types.length - 1)];
    }

    public static Double randomDouble(int min, int max){
        Random r = new Random();
       return min + (max - min) * r.nextDouble();
    }

    public static int RandomInt(int min, int max) {
        return (int) (Math.random() * (max - min + 1) + min);
    }

    //takes a list of columns and picks a random column name
    public static String pickKey(Hashtable<String, String> table) {
        Set s = table.keySet();
        Iterator loop = s.iterator();
        String[] arr = new String[s.size()];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = (String) loop.next();
        }
        String item = arr[RandomInt(0, arr.length - 1)];

        while (table.get(item).equals(types[2])){
            item = arr[RandomInt(0, arr.length - 1)];
        }

        return item;
    }

    public static boolean randomBool(){
        int val = RandomInt(0,1);
        return val == 1;
    }

    public static myPolygon randomPolygon(int length){  //polygon with the number of desired vertices
        int[] x = new int[length];
        int[] y = new int[length];

        for (int i = 0; i < length; i++) { //random point from (-50,-50) to (50,50)
            x[i] = RandomInt(-50,50);
            y[i] = RandomInt(-50,50);
        }
        return new myPolygon(new Polygon(x,y,length)); //wrap the polygon
    }

    public static Date RandomDate() throws ParseException { //random date between 1/1/1900 to 1/1/2045
        String ret = "";

        ret+= RandomInt(1900,2045)+ "-"; //random year 1900-2045

        int month = RandomInt(1,31); //random month
        if (month < 10) ret+= "0";
        ret += month + "-";

        int day = RandomInt(1,31); //random day
        if (day < 10) ret+= "0";
        ret += day + " ";

        ret +="00:00:00";

        Date val = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss").parse(ret);

        return val;
        }

    public static void Insert(String table,int amount) throws DBAppException, ParseException { //takes table name and the amount of tuples to be inserted

        for (int i = 0; i < amount; i++) {
            Hashtable<String, Object> tuple = createTuple(table); //create tuple
            DB.insertIntoTable(table,tuple); //perform insertion
            tableVals.get(table).add(tuple); //save value
        }

    }

    public static Hashtable<String, Object> createTuple(String table) throws ParseException {

        Hashtable<String, Object> tuple = new Hashtable<>(); //empty tuple

        Set keys = columns.get(table).keySet();  //a set of column names in a table
        Iterator loop = keys.iterator();

        while (loop.hasNext()){ //for every column
            String col = (String) loop.next(); //parse column name to string
            String type = columns.get(table).get(col); //get column type

            //add value according to the datatype
            if (type.equals(types[0])){ //int
                tuple.put(col, RandomInt(0,100000000));
            }
            if (type.equals(types[1])){ //str
                tuple.put(col, randomString(RandomInt(0,100)));
            }
            if (type.equals(types[2])){ //bool
                tuple.put(col, randomBool());
            }
            if (type.equals(types[3])){ //date
                tuple.put(col, RandomDate());
            }
            if (type.equals(types[4])){ //double
                tuple.put(col, randomDouble(0,100));
            }
            if (type.equals(types[5])){ //polygon
                tuple.put(col, randomPolygon(RandomInt(3,15)));
            }
        }
        return tuple;
    }
}