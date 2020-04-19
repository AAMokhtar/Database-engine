package DatabaseEngine.testing;
import java.awt.Polygon;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;

import DatabaseEngine.*;

public class GeneralTest {
	
	
	//where the testing happens!
	public static void main (String[] args) throws DBAppException {
		DBApp tester = new DBApp();
		
		tester.init();
		
		Hashtable<String, String> testTable = new Hashtable<String,String>();
		Hashtable<String, String> testTable2 = new Hashtable<String,String>();

		testTable.put("id", "java.lang.Integer");
		testTable.put("name", "java.lang.String");
		testTable.put("height", "java.lang.Double");
		testTable.put("isMarried", "java.lang.Boolean");
		testTable.put("birthday", "java.util.Date");
		testTable.put("ResidenceArea", "java.awt.Polygon");
		
		testTable2.put("shape_id", "java.lang.Integer");
		testTable2.put("name", "java.lang.String");
		testTable2.put("height", "java.lang.Double");
		testTable2.put("isQuadrilateral", "java.lang.Boolean");
		testTable2.put("IDKReally", "java.util.Date");
		testTable2.put("Shape", "java.awt.Polygon");

        
        
        
        
        
        
        
        
        
		
		
		
		//createtable - createindexes re-do
//		tester.createTable("Shape", "Shape", testTable2);
//		tester.createTable("Citizen","id",testTable);
//		tester.createRTreeIndex("Citizen", "ResidenceArea");
//		tester.createBTreeIndex("Shape","height");

		
		//create table tests
		//0 - only this should pass
		//tester.createTable("Citizen","id",testTable);
		
		//1
		//tester.createTable("Citizen","id",testTable);
		//2
		//tester.createTable("itizen","idd",testTable);
		//3
		//testTable.put("Residence", "java.awt.Plygon");
		//tester.createTable("itizen","id",testTable);
		
		//create B index test
		//0 
		//tester.createBTreeIndex("hola", "ResidenceArea");
		//1
		//tester.createBTreeIndex("Citizen", "residenceArea");
		//2
		//tester.createBTreeIndex("Citizen", "ResidenceArea");
		//3 - only this should pass 
		//tester.createBTreeIndex("Citizen", "height");
		//done: create an index after tuples inserted
		
		
		//create R index test
		//0 
		//tester.createRTreeIndex("hola", "ResidenceArea");
		//1
		//tester.createRTreeIndex("Citizen", "residenceArea");
		//2
		//tester.createRTreeIndex("Citizen", "id");
		//3 - only this should pass 
		//tester.createRTreeIndex("Citizen", "ResidenceArea");
		//done: create an index after tuples inserted
		
		
		//CITIZEN

		//row 1, 2
		Hashtable<String,Object> insert = new Hashtable<String,Object>();
		int[] x = {1,2,3,4};
		int[] y = {4,5,6,7};
		
        insert.put("id", new Integer(2));
        insert.put("name", "Basant");
        insert.put("height", new Double(175.01));
        insert.put("isMarried", new Boolean(false));
        insert.put("birthday",new Date(1999,5,27));
        insert.put("ResidenceArea",new Polygon(x,y,4));
        
        //row 3
        Hashtable<String,Object> insert1 = new Hashtable<String,Object>();
		int[] q = {1,2,3,4};
		int[] w = {4,5,6,7};
        insert1.put("id", new Integer(3));
        insert1.put("name", "Chantelle");
        insert1.put("height", new Double(175.01));
        insert1.put("isMarried", new Boolean(false));
        insert1.put("birthday",new Date(1999,5,27));
        insert1.put("ResidenceArea",new Polygon(q,w,4));
        
        //row 4
        Hashtable<String,Object> insert2 = new Hashtable<String,Object>();
		int[] e = {1,2,3,4};
		int[] r = {4,5,6,7};
        insert2.put("id", new Integer(3));
        insert2.put("name", "Reigna");
        insert2.put("height", new Double(175.01));
        insert2.put("isMarried", new Boolean(false));
        insert2.put("birthday",new Date(1999,5,27));
        insert2.put("ResidenceArea",new Polygon(e,r,4));
        
        //row 5
        Hashtable<String,Object> insert3 = new Hashtable<String,Object>();
		int[] t = {1,2,3,4};
		int[] u = {4,5,6,7};
        insert3.put("id", new Integer(4));
        insert3.put("name", "Reigna");
        insert3.put("height", new Double(174.01));
        insert3.put("isMarried", new Boolean(false));
        insert3.put("birthday",new Date(1999,5,27));
        insert3.put("ResidenceArea",new Polygon(t,u,4));
        
        //row 6
        Hashtable<String,Object> insert4 = new Hashtable<String,Object>();
		int[] v = {1,2,3,4};
		int[] b = {4,5,6,7};
        insert4.put("id", new Integer(5));
        insert4.put("name", "Soufiane");
        insert4.put("height", new Double(173.01));
        insert4.put("isMarried", new Boolean(false));
        insert4.put("birthday",new Date(1999,5,27));
        insert4.put("ResidenceArea",new Polygon(v,b,4));
        
        //row 7 
        Hashtable<String,Object> insert5 = new Hashtable<String,Object>();
		int[] d = {1,2,3,4};
		int[] n = {4,5,6,7};
        insert5.put("id", new Integer(6));
        insert5.put("name", "Zayn");
        insert5.put("height", new Double(172.01));
        insert5.put("isMarried", new Boolean(false));
        insert5.put("birthday",new Date(1999,5,27));
        insert5.put("ResidenceArea",new Polygon(d,n,4));
        
        Hashtable<String,Object> violate1 = new Hashtable<String,Object>();
		int[] dd = {1,2,3,4};
		int[] nn = {4,5,6,7};
        insert5.put("id", new Integer(6));
        insert5.put("name", "Zayn");
        insert5.put("height", new Double(175.01));
        insert5.put("isMarried", new Boolean(false));
        insert5.put("birthday",new Date(1999,5,27));
        
        Hashtable<String,Object> violate2 = new Hashtable<String,Object>();

        insert5.put("name", "Zayn");
        insert5.put("height", new Double(175.01));
        insert5.put("isMarried", new Boolean(false));
        insert5.put("birthday",new Date(1999,5,27));
        
        
        
        //POLYGON
        
        
		
		Hashtable<String,Object> insertshape1 = new Hashtable<String,Object>();
		int[] tt = {1,2,3,4};
		int[] uu = {4,5,6,7};
		
		insertshape1.put("shape_id", new Integer(1));
		insertshape1.put("name", "Square");
		insertshape1.put("height", new Double(132.01));
		insertshape1.put("isQuadrilateral", new Boolean(true));
		insertshape1.put("IDKReally",new Date(1999,5,27));
        insertshape1.put("Shape",new Polygon(tt,uu,4));
        
        
    	Hashtable<String,Object> insertshape2 = new Hashtable<String,Object>();
		int[] ttt = {7,22,33,44};
		int[] uuu = {8,55,66,77};
		insertshape2.put("shape_id", new Integer(2));
		insertshape2.put("name", "Circle");
		insertshape2.put("height", new Double(2.01));
		insertshape2.put("isQuadrilateral", new Boolean(false));
		insertshape2.put("IDKReally",new Date(2134,5,27));
        insertshape2.put("Shape",new Polygon(ttt,uuu,4));
        
        
        Hashtable<String,Object> insertshape3 = new Hashtable<String,Object>();
		int[] tttt = {7,6,3,4};
		int[] uuuu = {8,7,6,7};
		
		insertshape3.put("shape_id", new Integer(3));
		insertshape3.put("name", "Triangle");
		insertshape3.put("height", new Double(24.1));
		insertshape3.put("isQuadrilateral", new Boolean(false));
		insertshape3.put("IDKReally",new Date(2132,6,67));
        insertshape3.put("Shape",new Polygon(tttt,uuuu,4));
        
        
        Hashtable<String,Object> insertshape4 = new Hashtable<String,Object>();
		int[] ttttt = {1,2,3,4};
		int[] uuuuu = {0,5,6,7};
		insertshape4.put("shape_id", new Integer(4));
		insertshape4.put("name", "Pentagon");
		insertshape4.put("height", new Double(23.01));
		insertshape4.put("isQuadrilateral", new Boolean(false));
		insertshape4.put("IDKReally",new Date(4325,5,27));
        insertshape4.put("Shape",new Polygon(ttttt,uuuuu,4));
        
        
        Hashtable<String,Object> insertshape5 = new Hashtable<String,Object>();
		int[] tttttt = {7,2,3,6};
		int[] uuuuuu = {8,5,6,7};
		insertshape5.put("shape_id", new Integer(5));
		insertshape5.put("name", "Circle");
		insertshape5.put("height", new Double(2.01));
		insertshape5.put("isQuadrilateral", new Boolean(false));
		insertshape5.put("IDKReally",new Date(2134,5,27));
        insertshape5.put("Shape",new Polygon(tttttt,uuuuuu,4));
        
        
        Hashtable<String,Object> insertshape6 = new Hashtable<String,Object>();
		int[] ttttttt = {7,2,3,4};
		int[] uuuuuuu = {8,5,6,7};
		insertshape6.put("shape_id", new Integer(6));
		insertshape6.put("name", "Rhombus");
		insertshape6.put("height", new Double(8.7));
		insertshape6.put("isQuadrilateral", new Boolean(true));
		insertshape6.put("IDKReally",new Date(2134,5,27));
        insertshape6.put("Shape",new Polygon(ttttttt,uuuuuuu,4));
        
        
        //insert tests
        //0 - pass + sorted + correct tables
//        tester.insertIntoTable("Citizen",insert2);
//        tester.insertIntoTable("Citizen",insert);
//        tester.insertIntoTable("Citizen",insert1);
//        tester.insertIntoTable("Shape",insertshape2);
//        tester.insertIntoTable("Shape",insertshape1);
//        tester.insertIntoTable("Citizen",insert3);
//        tester.insertIntoTable("Citizen",insert3);
//        tester.insertIntoTable("Citizen",insert4);
//        tester.insertIntoTable("Citizen",insert5);
//        tester.insertIntoTable("Citizen",insert5);
//        tester.insertIntoTable("Shape",insertshape2);
//        tester.insertIntoTable("Shape",insertshape1);
//        tester.insertIntoTable("Shape",insertshape2);
//        tester.insertIntoTable("Shape",insertshape1);
 //       tester.insertIntoTable("Shape",insertshape2);
//        tester.insertIntoTable("Shape",insertshape1);
//        1 - must fail
//        tester.insertIntoTable("Citizen", violate1);
//        2 - must fail
//        tester.insertIntoTable("Citizen", violate2);
        
        
        

        //TODO delete birthday
            
        Hashtable<String, Object> deletecit1  = new Hashtable<String, Object>();
        //update tests
        //0 - must fail
        deletecit1.put("name", "Basant");
		deletecit1.put("id", new Integer(3));
		//tester.updateTable("Citizen","2", deletecit1);
		//1 - must fail
		deletecit1.clear();
		deletecit1.put("height", new Integer(123));
		deletecit1.put("name", "Basant");
		//tester.updateTable("Citizen","3", deletecit1);
		
		//2 - must pass
		deletecit1.clear();
		deletecit1.put("height", new Double(123.0));
		deletecit1.put("name", "Basant");
		//tester.updateTable("Citizen","3", deletecit1);
		
		//2.5 - must fail
		deletecit1.clear();
		deletecit1.put("height", new Double(123.0));
		deletecit1.put("name", "Basant");
		//tester.updateTable("Citizen", "13", deletecit1);
		
		
		//3- must fail
//		deletecit1.clear();
//		deletecit1.clear();
//		deletecit1.put("shape_idd", new Integer(9));
//		deletecit1.put("name", "999999");
//		deletecit1.put("height", new Double(9.99));
		
		//tester.updateTable("Shape","3", deletecit1);
		
		
		//4 - must fail
		//int[] ba = {7,22,33,44};
//		//int[] baa = {8,55,66,77};
//		deletecit1.clear();
//		//deletecit1.put("Shape",new Polygon(ba,baa,4));
//		deletecit1.put("shape_id", new Integer(9));
//		deletecit1.put("name", "999999");
//		deletecit1.put("height", new Double(9.99));
//		
//		//tester.updateTable("Shape","3", deletecit1);

		
		//5 - must pass
//		int[] ba = {7,22,33,44};
//		int[] baa = {8,55,66,77};
		deletecit1.clear();
		//deletecit1.put("shape_id", new Integer(9));
		deletecit1.put("name", "Rectangle");
		deletecit1.put("height", new Double(9.99));
		//tester.updateTable("Shape","(1,4),(2,5),(3,6),(4,7)", deletecit1);
//		
	

		
		
        Hashtable<String, Object> del  = new Hashtable<String, Object>();

		//delete test cases
        
        //0 - fail
        del.put("name", "Basant");
		del.put("id", new Integer(3));
		//tester.deleteFromTable("Citizen", del);
		
		//1- fail
		del.clear();
		del.put("name", "Basant");
		del.put("id", new Integer(3));
		//tester.deleteFromTable("tableNotExisting", del);
		
		
		//2 - pass (delete only with basant and id 3)
		del.clear();
		del.put("name", "Basant");
		del.put("id", new Integer(3));
		//tester.deleteFromTable("Citizen", del);
		
		
		//3 - pass (delete the page too)
		del.clear();
		del.put("name", "Basant");
		//tester.deleteFromTable("Citizen", del);
		
		//4 - must fail.
		//tester.deleteFromTable("Citizen", del);
		
		
		//5 - delete from another table - TODO
		del.clear();
		//del.put("shape_id", new Integer(2));
		del.put("name", "Circle");
		//del.put("height", new Double(2.01));
		//tester.createRTreeIndex("Shape","Shape");
		//tester.deleteFromTable("Shape", del);
		
		
		//6 - insert then make an index on isMarried, then delete? check!
		
		//tester.insertIntoTable("Citizen", insert4);
		//tester.insertIntoTable("Citizen", insert5);
		//tester.createBTreeIndex("Citizen","isMarried");
		
		//del.clear();
		//del.put("isMarried", new Boolean(false));
		//tester.deleteFromTable("Citizen",del);
		
		//7 empty other table
//		del.clear();
//		del.put("height", new Double(9.99));
//		tester.deleteFromTable("Shape", del);
		
		
	
		
		
		
		/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		///MEGA TESTS!
		
//	DELETE ALL THEN INSERT ALL
//      tester.insertIntoTable("Citizen",insert2);
//      tester.insertIntoTable("Shape",insertshape6);
//      tester.insertIntoTable("Citizen",insert);
//      tester.insertIntoTable("Shape",insertshape4);
//      tester.insertIntoTable("Citizen",insert1);
//      tester.insertIntoTable("Shape",insertshape2);
//      tester.insertIntoTable("Shape",insertshape1);
//      tester.insertIntoTable("Citizen",insert3);
//      tester.insertIntoTable("Shape",insertshape3);
//      tester.insertIntoTable("Citizen",insert4);
//      tester.insertIntoTable("Shape",insertshape5);
//      tester.insertIntoTable("Citizen",insert5);
//      tester.insertIntoTable("Citizen",insert2);
//      tester.insertIntoTable("Shape",insertshape6);
//      tester.insertIntoTable("Citizen",insert);
//      tester.insertIntoTable("Shape",insertshape4);
//      tester.insertIntoTable("Citizen",insert1);
//      tester.insertIntoTable("Shape",insertshape2);
//      tester.insertIntoTable("Shape",insertshape1);
//      tester.insertIntoTable("Citizen",insert3);
//      tester.insertIntoTable("Shape",insertshape3);
      
		
		//REFERENCE
//		testTable.put("id", "java.lang.Integer");
//		testTable.put("name", "java.lang.String");
//		testTable.put("height", "java.lang.Double");
//		testTable.put("isMarried", "java.lang.Boolean");
//		testTable.put("birthday", "java.util.Date");
//		testTable.put("ResidenceArea", "java.awt.Polygon");
//		
//		testTable2.put("shape_id", "java.lang.Integer");
//		testTable2.put("name", "java.lang.String");
//		testTable2.put("height", "java.lang.Double");
//		testTable2.put("isQuadrilateral", "java.lang.Boolean");
//		testTable2.put("IDKReally", "java.util.Date");
//		testTable2.put("Shape", "java.awt.Polygon");
//		tester.createBTreeIndex("Citizen", "id");
//		tester.createBTreeIndex("Citizen", "name");
//		tester.createBTreeIndex("Citizen", "isMarried");
//		tester.createBTreeIndex("Citizen", "birthday");
//		tester.createRTreeIndex("Shape", "Shape");



      
//      tester.insertIntoTable("Citizen",insert4);
//    tester.insertIntoTable("Citizen",insert5);
//    tester.insertIntoTable("Citizen",insert);
//    tester.insertIntoTable("Citizen",insert2);
//    tester.insertIntoTable("Citizen",insert1);
//    tester.insertIntoTable("Citizen",insert3);
//      tester.insertIntoTable("Shape",insertshape5);
//      tester.insertIntoTable("Shape",insertshape6);
//      tester.insertIntoTable("Shape",insertshape4);
//      tester.insertIntoTable("Shape",insertshape2);
//      tester.insertIntoTable("Shape",insertshape1);
//      tester.insertIntoTable("Shape",insertshape3);
//      tester.insertIntoTable("Citizen",insert4);
//      tester.insertIntoTable("Shape",insertshape5);
//      tester.insertIntoTable("Citizen",insert5);
//      tester.insertIntoTable("Citizen",insert2);
//      tester.insertIntoTable("Shape",insertshape6);
//      tester.insertIntoTable("Citizen",insert);
//      tester.insertIntoTable("Shape",insertshape4);
//      tester.insertIntoTable("Citizen",insert1);
//      tester.insertIntoTable("Shape",insertshape2);
//      tester.insertIntoTable("Shape",insertshape1);
//      tester.insertIntoTable("Citizen",insert3);
//      tester.insertIntoTable("Shape",insertshape3);
//      tester.insertIntoTable("Citizen",insert4);
//      tester.insertIntoTable("Shape",insertshape5);
//      tester.insertIntoTable("Citizen",insert5);
//		
//update all of citizen table
		
		//update all married to be true, and back to false
		Hashtable<String,Object> up = new Hashtable<String,Object>();
		up.put("isMarried", new Boolean(true));
//		tester.updateTable("Citizen", "2", up);
		
		
		//update shape with false
		//REFERENCE
//		insertshape1.put("shape_id", new Integer(1));
//		insertshape1.put("name", "Square");
//		insertshape1.put("height", new Double(132.01));
//		insertshape1.put("isQuadrilateral", new Boolean(true));
//		insertshape1.put("IDKReally",new Date(1999,5,27));
//        insertshape1.put("Shape",new Polygon(tt,uu,4));
		
		//test begins here
		up.clear();
		up.put("isQuadrilateral", new Boolean(true));
		//tester.updateTable("Shape", "(1,4),(2,5),(3,6),(4,7)", up);
		
		//update then delete based on date
		up.clear();
		up.put("birthday", new Date(1999,5,27));
		//tester.deleteFromTable("Citizen", up);
		//tester.updateTable("")
		
		//update based on date, and delete all
		//tester.
		
//		up.clear();
//		up.put("isQuadrilateral", new Boolean(true));
//		tester.deleteFromTable("Shape", up);
//		
//		for (int i = 0; i<50;i++) {
//			tester.insertIntoTable("Citizen",insert2);
//			tester.insertIntoTable("Citizen",insert3);
//			tester.insertIntoTable("Shape", insertshape3);
//			tester.insertIntoTable("Shape", insertshape4);
//		}
		
		up.clear();
		up.put("name", "Basant");
		//tester.updateTable("Citizen","3",up);
		up.clear();
		up.put("name", "Pentagon");
		//tester.updateTable("Shape", "(7,8),(6,7),(3,6),(4,7)", up);
		//tester.deleteFromTable("Shape",up);
		
		
		
		//next: update them done
		// another delete all done
		//update on one repeated value - delete on it - table should be empty (isMarried in citizens) done
		//ALWAYS CHECK B+ NODES! done 
		
		
				//create a 3rd table, insert in it, update, then delete (dup)
			
				
		Hashtable<String,String> testTabl = new Hashtable<String,String>();
		
		testTabl.put("double", "java.lang.Double");
		testTabl.put("boolean", "java.lang.Boolean");
		testTabl.put("date", "java.util.Date");
		testTabl.put("polygon", "java.awt.Polygon");
		
		
		
		up.clear();
		up.put("double", new Double(3.0));
		up.put("date", new Date(-666,11,3));
		up.put("polygon", new Polygon());
		up.put("boolean", new Boolean(false));
		
		//tester.insertIntoTable("table3", up);
//		tester.insertIntoTable("Shape", insertshape3);
//		tester.insertIntoTable("Shape", insertshape3);
//		tester.insertIntoTable("Shape", insertshape3);
//		tester.insertIntoTable("Shape", insertshape3);
		
		
		
		
		//tester.createTable("table3","date", testTabl);
//		tester.insertIntoTable("table3", up);
//		tester.insertIntoTable("table3", up);
//		tester.insertIntoTable("table3", up);
//		tester.insertIntoTable("table3", up);
		
		up.clear();
		up.put("double", new Double(999.9));
		
		//tester.updateTable("table3", "1234-12-03", up);
		
		up.clear();
		
		up.put("date", new Date(-666,11,3));
		
		//up.clear();
		//up.put("polygon", new Polygon(ttt,uuu,3));
		//tester.deleteFromTable("table3", up);
		
		//then delete based on date DONE.
		//done: delete - update - insert
		
		
		//update - delete (updated) - insert 
	
		up.clear();
		up.put("name","Ayah");
		
		//tester.updateTable("Citizen","3",up);
		//tester.insertIntoTable("Citizen", insert);
		
		up.clear();
		up.put("name", "Juniper");
		//tester.updateTable("Citizen", "4", up);
		//tester.deleteFromTable("Citizen", up);
		//tester.insertIntoTable("Citizen", insert5);
		
		System.out.println("CITIZEN");
		printPages("Citizen");
		System.out.println("SHAPE");
		printPages("Shape");
		System.out.println("TABLE3");
		printPages("table3");
		
		
		
	}
	
	public static void printPages(String name) {

		Table obj=Utilities.deserializeTable(name);


		for (int i = 0; i < obj.getPages().size(); i++) {
			Page p=Utilities.deserializePage(obj.getPages().get(i));
			System.out.println("id: " + p.getID());
			System.out.println(p);
			Utilities.serializePage(p);
		}

		Utilities.serializeTable(obj);
	}
	
	
	

}
