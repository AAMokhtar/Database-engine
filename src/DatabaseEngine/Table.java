package DatabaseEngine;

import java.awt.*;
import java.io.*;
import java.util.*;

public class Table implements Serializable{

	private Vector<Integer> pagesGroup;
	private String tableName;


	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException, IOException {
		
		
		/*hashtable demo!
		Hashtable<String, String> t = new Hashtable<String, String>();
		t.put("1","One");
		t.put("2", "Two");
		System.out.println(t);
		
		Set<String> e = t.keySet();
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
	public Page createNewPage() throws IOException {
		//ONLY through a table I can create a page.
		Page p = new Page();
		if(pagesGroup.size()!=0 && p.getID()!=pagesGroup.get(pagesGroup.size()-1)+1)
		{
			p.setID(pagesGroup.get(pagesGroup.size()-1)+1);
		}
		Utilities.serializePage(p);
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

	//method used to determine which in which of the table pages should I insert in
	//METHOD WORKS. IT HAS BEEN REVIEWED
	public Page determinePageNeededForInsert(int indexClusteringKey,Object clusteringKey, String clusteringKeyType, Vector<Object> newTuple) throws IOException, ClassNotFoundException {
		for (int i = 0; i < pagesGroup.size(); i++) {
			//deserialize page needed
			int pageID=pagesGroup.get(i);
			Page currentPageInMemory=Utilities.deserializePage(pageID);
			//determine if this is the correct page to insert tuple in
			boolean correctPage=currentPageInMemory.tupleToBePlacedInPage(indexClusteringKey, clusteringKey, clusteringKeyType);
			if(correctPage)
			{
				//serialize
				Utilities.serializePage(currentPageInMemory);
				return currentPageInMemory;
			}
			//serialize
			Utilities.serializePage(currentPageInMemory);
			//special case: If this page is not full due to Saeed's deletion
			if(i!=pagesGroup.size()-1 && currentPageInMemory.getElementsCount()<currentPageInMemory.getN())
			{
				Page nextPage=Utilities.deserializePage(pagesGroup.get(i+1));
				if(clusteringKeyType.equals("java.lang.Integer"))
				{
					Integer intObj=(Integer)nextPage.getTupleFromPage(0).get(indexClusteringKey);
					Integer myintObj=(Integer) clusteringKey;
					if(myintObj<=intObj)
					{
						currentPageInMemory.insertIntoPage(newTuple);
						Utilities.serializePage(currentPageInMemory);
						return new Page();
					}
				}
				else if(clusteringKeyType.equals("java.lang.String"))
				{
					String strObj=(String)nextPage.getTupleFromPage(0).get(indexClusteringKey);;
					String mystrObj=(String) clusteringKey;
					if(mystrObj.compareTo(strObj)<=0)
					{
						currentPageInMemory.insertIntoPage(newTuple);
						Utilities.serializePage(currentPageInMemory);
						return new Page();
					}
				}
				else if(clusteringKeyType.equals("java.lang.Double"))
				{
					Double dblObj=(Double)nextPage.getTupleFromPage(0).get(indexClusteringKey);;
					Double mydblObj=(Double) clusteringKey;
					if(mydblObj.compareTo(dblObj)<=0)
					{
						currentPageInMemory.insertIntoPage(newTuple);
						Utilities.serializePage(currentPageInMemory);
						return new Page();
					}
				}
				else if(clusteringKeyType.equals("java.util.Date"))
				{
					Date dateObj=(Date)nextPage.getTupleFromPage(0).get(indexClusteringKey);
					Date mydateObj=(Date) clusteringKey;
					if(mydateObj.compareTo(dateObj)<=0)
					{
						currentPageInMemory.insertIntoPage(newTuple);
						Utilities.serializePage(currentPageInMemory);
						return new Page();
					}
				}
				else if(clusteringKeyType.equals("java.awt.Polygon"))
				{
					myPolygon polyObj=new myPolygon((Polygon)nextPage.getTupleFromPage(0).get(indexClusteringKey));
					myPolygon myPolyObj= new myPolygon((Polygon)clusteringKey);
					if(myPolyObj.compareTo(polyObj)<=0)
					{
						currentPageInMemory.insertIntoPage(newTuple);
						Utilities.serializePage(currentPageInMemory);
						return new Page();
					}
				}
				else if(clusteringKeyType.equals("java.lang.Boolean"))
				{
					Boolean boolObj=(Boolean)nextPage.getTupleFromPage(0).get(indexClusteringKey);
					Boolean myBoolObj= ((Boolean)clusteringKey);
					if(Boolean.compare(myBoolObj,boolObj)<=0)
					{
						currentPageInMemory.insertIntoPage(newTuple);
						Utilities.serializePage(currentPageInMemory);
						return new Page();
					}
				}
			}
		}
		return null;
	}

	//special case of insert, where tuple to be inserted is last one in entire table or table is still empty
	////METHOD WORKS. IT HAS BEEN REVIEWED
	public void insertSpecialCase(Vector<Object> newTuple) throws IOException, ClassNotFoundException {
		if(pagesGroup.size()==0)
		{
			//table is still empty
			Page newPage= createNewPage();
			newPage.insertIntoPage(newTuple);
			//serialize again to save changes
			Utilities.serializePage(newPage);
			return;

		}
		Page lastPage= Utilities.deserializePage(pagesGroup.get(pagesGroup.size()-1));
		if(lastPage.getElementsCount()<lastPage.getN())
		{
			//last page is not full. Tuple will be placed in that page
			lastPage.insertIntoPage(newTuple);

		}
		else
		{
			//last page is full. Create new page for that tuple.
			Utilities.serializePage(lastPage);
			lastPage= createNewPage();
			lastPage.insertIntoPage(newTuple);
		}
		//serialize again to save changes
		Utilities.serializePage(lastPage);
	}
	public int getPageIndx(Page p)
	{
		for (int i = 0; i < pagesGroup.size(); i++) {
			if(pagesGroup.get(i)==p.getID())
			{
				return i;
			}
		}
		return -1;
	}

	//in insertion I create new pages, insert tuple in them and delete old pages
	//METHOD WORKS. IT HAS BEEN REVIEWED
	public void insertRegularCase(Vector<Object> newTuple, Page currentPageInMemory,int indexClusteringKey,Object clusteringKey, String clusteringKeyType) throws IOException, ClassNotFoundException {
		int indxOfNewRow = currentPageInMemory.binarySearch(indexClusteringKey, clusteringKey, clusteringKeyType);
		//get indx of current page in memory
		int i=getPageIndx(currentPageInMemory);
		//start insertion process
		int size=currentPageInMemory.getElementsCount();
		for (int j = 0; j < size; j++) {
			if(j==indxOfNewRow)
			{
				currentPageInMemory.insertIntoPage(newTuple);
			}
			currentPageInMemory.insertIntoPage(currentPageInMemory.getTupleFromPage(0));
			currentPageInMemory.deleteFirst();
		}
		Utilities.serializePage(currentPageInMemory);
		int currentPage=i+1;
		while(currentPageInMemory.getElementsCount()>currentPageInMemory.getN())
		{
			Vector<Object> row=currentPageInMemory.deleteLast();
			Utilities.serializePage(currentPageInMemory);
			if(currentPage==pagesGroup.size())
			{
				currentPageInMemory=createNewPage();
				currentPageInMemory.insertIntoPage(row);
			}
			else
			{
				currentPageInMemory=Utilities.deserializePage(pagesGroup.get(currentPage));
				currentPageInMemory.insertIntoPage(row);
				size=currentPageInMemory.getElementsCount();
				for (int j = 1; j < size; j++) {
					currentPageInMemory.insertIntoPage(currentPageInMemory.getTupleFromPage(0));
					currentPageInMemory.deleteFirst();
				}
			}
			Utilities.serializePage(currentPageInMemory);
			currentPage++;
		}

	}
	
	public void delete(Hashtable<String, Object> htblColNameValue) throws IOException, ClassNotFoundException, DBAppException{
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
			throw new DBAppException("Invalid Columns");
		}

		// Looping on all the pages to check for the elements to be deleted
		// Deleting all the matching elements
		// Checking if the page is empty, if it is, deleting the page
		for (int i = 0; i < pagesGroup.size(); i++) {
			Page p = Utilities.deserializePage(pagesGroup.get(i));
			p.deleteByValue(keyValue);
			if (p.getPageElements().size() == 0) {
				pagesGroup.remove(i);
				i--;
				deletePage(p);
			} else {
				Utilities.serializePage(p);
			}

		}
		Utilities.serializeTable(this);

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
		File f = new File("data//" + "page_" + pageID + ".class");
		f.delete();
	}


	public String getName() {
		return tableName;
	}

	//FOR TESTING PURPOSES
	public static void main(String args[])  {

		Hashtable<String, String> t = new Hashtable<String, String>();
		t.put("1","One");
		t.put("2", "Two");
		System.out.println(t);
	}
}
