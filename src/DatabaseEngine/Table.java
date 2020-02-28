package DatabaseEngine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Table implements Serializable{
	
	private Vector<Integer> pagesGroup; 
	private String tableName;
	

	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException {
		
		
		/*hashtable demo!
		Hashtable<String, String> t = new Hashtable<String, String>();
		t.put("1","One");
		t.put("2", "Two");
		System.out.println(t);
		
		DatabaseEngine.Set<String> e = t.keySet();
		for(String i : e) {
			System.out.println(i + " : " + (t.get(i)));
		}*/
		
		//step 0: check if table already exists
		if(Utilities.isTableUnique(strTableName)) {
			
	
		tableName =strTableName;
		//step one: check if all the column types are acceptable.
		Set<String> colName = htblColNameType.keySet();
		
		for (String n : colName) {
			if(checkApplicable(htblColNameType.get(n))==false) {
				throw new DBAppException("Inapplicable column type.");
			}			
		}
		
		//step two: start writing into metadata file.
		Utilities.writeHeaderIntoMetaData(htblColNameType, strTableName, strClusteringKeyColumn);
		
		pagesGroup =  new Vector<Integer>();}
		
		else {
			System.out.println("Table already exists.");
		}
	}
	
	
	/*
	 * TODO: FOR ALI: use this to create new pages if insert warrants so.
	 */
	public Page createNewPage() {
		//ONLY through a table I can create a page.
		Page p = new Page();
		pagesGroup.add(p.getID());
		return p;
	}
	
	
	//return the array of ids of pages involved in this table
	public Vector<Integer> getPages(){
		return pagesGroup;
	}
	
	
	
	//verify column type correctness. USED FOR METADATA FILE.
	//a helper getPossibleTypes is used.
	public static boolean checkApplicable(String s) {
		Vector<String> types = Utilities.getPossibleTypes();
		for (String possible : types) {
			if (possible.equals(s)) {
				return true;
			}
		}
		
		return false;
	}
	
	
	//TODO: for Saeed: you might find this useful.
	public void deletePage(Page P) {
		if (P.getElementsCount() ==0) {
			pagesGroup.remove(P.getID());
			//TODO: but check how are you gonna free it from memory: either you do tombstone, or you shift up. Check with Dr. Wael
		}
	}
	
	
	public String getName() {
		return tableName;
	}
	
	//FOR TESTING PURPOSES
	public static void main(String args[])  {
		
	    try {
		Hashtable<String, String> t = new Hashtable<String, String>();
		t.put("1","One");
		t.put("2", "Two");
		System.out.println(t);
	    }
	    
	    catch(Exception E) {
	    	System.out.println("for debugging, check TABLE class.");
	    	E.printStackTrace();
	    }

		
		
	}
	
	
}
