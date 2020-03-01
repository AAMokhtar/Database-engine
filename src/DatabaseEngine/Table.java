package DatabaseEngine;

import java.io.*;
import java.util.ArrayList;
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
	

	
	public void delete(Hashtable<String, Object> htblColNameValue) {
		// checking if the entered hashtable has columns matching the table and
		// populating Hashtable with integers for keys for later usage
		// and checking if all requested columns for delete are valid
		ArrayList<String[]> data = Utilities.readMetaDataForSpecificTable(tableName);
		Hashtable<Integer, Object> keyValue = new Hashtable<Integer, Object>();
		Set<String> keys = htblColNameValue.keySet();
		for (String key : keys) {
			for (int i = 0; i < data.size(); i++) {
				if (data.get(i)[1].equals(key)) {
					keyValue.put(i, htblColNameValue.get(key));
				}
			}
		}
		if (keyValue.size() != htblColNameValue.size()) {
			System.out.println("Failed to delete!");
			return;
		}

		// Looping on all the pages to check for the elements to be deleted
		// Deleting all the matching elements
		// Checking if the page is empty, if it is, deleting the page
		for (int i = 0; i < pagesGroup.size(); i++) {
			Page p = Utilities.deserializePage(pagesGroup.get(i));
			p.deleteByValue(keyValue);
			if (p.getElementsCount() == 0) {
				deletePage(p);
			} else {
				Utilities.serializePage(p);
			}
	
		}

		// populating pages with empty rows

/*		for (int i = 0; i + 1 < pagesGroup.size(); i++) {
			Page p1 = temp.get(i);
			int freeRows = p1.getN() - p1.getElementsCount();
			for (int j = 0; j < freeRows; j++) {
				Page p2 = temp.get(i + 1);
				p1.insertIntoPage(p2.getTupleFromPage(0));
				p2.deleteByIndex(0);
			}
		}
*/		// deleting all the empty pages and serializing the others
/*
		for (int i = 0; i + 1 < pagesGroup.size(); i++) {
			Page p = temp.get(i);
			if (p.getElementsCount() == 0) {
				temp.removeElementAt(i);
				deletePage(p);
			} else {
				Utilities.serializePage(p);
			}
		}*/
	}

	// Deleting the page completely
	public void deletePage(Page P) {
		int pageID = P.getID();
		try {
			File f = new File("data//" + "page_" + pageID + ".class");
			f.delete();
		} catch (Exception e) {
			e.printStackTrace();
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
