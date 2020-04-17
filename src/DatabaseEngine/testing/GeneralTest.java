package DatabaseEngine.testing;
import java.util.Hashtable;
import java.util.Set;

import DatabaseEngine.*;

public class GeneralTest {
	
	
	//where the testing happens!
	public static void main (String[] args) throws DBAppException {
		DBApp tester = new DBApp();
		
		//tester.init();
		
		Hashtable<String, String> testTable = new Hashtable<String,String>();

		testTable.put("id", "java.lang.Integer");
		testTable.put("name", "java.lang.String");
		testTable.put("height", "java.lang.Double");
		testTable.put("isMarried", "java.lang.Boolean");
		testTable.put("birthday", "java.util.Date");
		testTable.put("ResidenceArea", "java.awt.Polygon");
		
		
		/*//create table tests
		//0 - only this should pass
		tester.createTable("Citizen","id",testTable);
		//1
		tester.createTable("Citizen","id",testTable);
		//2
		tester.createTable("itizen","idd",testTable);
		//3
		testTable.put("Residence", "java.awt.Plygon");
		tester.createTable("itizen","id",testTable);*/
		
		//create B index test
		//0 
		//tester.createBTreeIndex("hola", "ResidenceArea");
		//1
		//tester.createBTreeIndex("Citizen", "residenceArea");
		//2
		//tester.createBTreeIndex("Citizen", "ResidenceArea");
		//3 - only this should pass TODO FAILED
		//tester.createBTreeIndex("Citizen", "height");
		//TODO: create an index after tuples inserted
		
		
		//create R index test
		//0 
		//tester.createRTreeIndex("hola", "ResidenceArea");
		//1
		//tester.createRTreeIndex("Citizen", "residenceArea");
		//2
		//tester.createRTreeIndex("Citizen", "id");
		//3 - only this should pass TODO failed
		//tester.createRTreeIndex("Citizen", "ResidenceArea");
		//TODO: create an index after tuples inserted
		
		
		//insertions
		
		tester.createTable("ccitizen","id",testTable);
		tester.createBTreeIndex("ccitizen", "height");
	}
	
	
	

}
