package DatabaseEngine; //change to team name before submitting

import java.util.Hashtable;

public class DBApp {

	public void init() {
		Utilities.initializeMetaData();
		//TODO: add any other "initializing code" here!
	}
	
	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType ) throws DBAppException {
		
		Table t = new Table(strTableName,strClusteringKeyColumn,htblColNameType);
		//now the programmer may initialize a page to insert into it.
		//insert into it using Tuples object
	}

	public static void main(String args[]) {
		//CREATE TABLE TEST PASSED!
		
//		Hashtable t = new Hashtable<String, String>();
//		t.put("ID","java.lang.Integer");
//		t.put("name","java.lang.String");
//		t.put("isAdult","java.lang.Boolean");
//		t.put("deathdate","java.util.Date");
//		t.put("gpa","java.lang.Double");
//		
//		DBApp d = new DBApp();

//       d.init();
//		try {
//			d.createTable("ESTUDIANTE", "name", t);
//		} catch (DBAppException e) {
//			System.out.println(e.getMessage());
//			e.printStackTrace();	}
	}
    
		
		//TODO: FOR ALL Y'ALL: PLEASE KEEP TESTS IN COMMENTS!! 
		
	

    
//Ali's part:

    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{


    }
//Mayar's part:

    public void updateTable(String strTableName, String strKey, Hashtable<String,Object> htblColNameValue)
            throws DBAppException{

    }
//Saeed's part:

    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{

    }



}
