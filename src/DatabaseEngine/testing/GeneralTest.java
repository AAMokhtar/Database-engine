package DatabaseEngine.testing;
import java.util.Hashtable;
import java.util.Set;

import DatabaseEngine.*;

public class GeneralTest {
	
	
	//where the testing happens!
	public static void main (String[] args) throws DBAppException {
		DBApp tester = new DBApp();
		
		tester.init();
		
		Hashtable<String, String> testTable = new Hashtable<String,String>();

		testTable.put("id", "java.lang.Integer");
		testTable.put("name", "java.lang.String");
		testTable.put("height", "java.lang.Double");
		testTable.put("isMarried", "java.lang.Boolean");
		testTable.put("birthday", "java.util.Date");
		testTable.put("ResidenceArea", "java.awt.Polygon");
		
		tester.createTable("Citizen","id",testTable);
		
		
		
		
		
		
		
	}
	
	
	

}
