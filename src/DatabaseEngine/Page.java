package DatabaseEngine;

import java.awt.*;
import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.*;


public class Page implements Serializable { 
	/*
	 * this class takes care of all of the affairs of a page. 
	 * THIS CLASS WAS TESTED.
	 */
	
	//fixed page characteristics
	private final int N = 5;
	private static int IDUpperBound = 0;
	
	//page important metadata
	private int count; //how many items in the page?
	private int pageID;
	//the actual page is a serialized vector of vectors (objects)
	private Vector<Vector> PageElements;

	
	//PURPOSE: CREATE A NEW PAGE
	public Page() {
		count = 0;
		pageID = ++IDUpperBound;
		PageElements = new Vector<Vector>();
	}
	

	
	//to be used for testing purposes only. DO NOT USE IN PROJECT IMPLEMNTATION!
	public void resetIDUpperBound() {
		IDUpperBound = 0;
	}

	public void deleteFirst() { PageElements.remove(0); }

	public Vector<Object> deleteLast()
	{
		return PageElements.remove(PageElements.size()-1);
	}

	//to know max number of records per page.
	public int getN() {
		return N;
	}
	
	
	//get the unique page identifier. useful for deseralizing (because which page do u want boi)
	public int getID() {
		return pageID;
	}

	public void setID(int ID)
	{
		pageID=ID;
	}

	//get number of elements in page
	public int getElementsCount() {
		return this.count;
	}
	
	//get the vector of records
	public Vector<Vector> getPageElements()
	{
		return this.PageElements;
	}

	
	/*
	 * TODO: FOR ALI: insert into page!
	 MUST UPDATE: 
	 * count of the items in this page in this class
	 * the page array in TABLE class (if having to create new page)
	 * the vector pageElements (due to new created tuple)
	 */

	public void insertIntoPage(Vector<Object> newTuple)
	{
		PageElements.add(newTuple);
		count=PageElements.size();

	}

	public static Vector<Object> initializeTime(Vector<Object> tuple)
	{
		tuple.add(LocalDateTime.now());
		return tuple;
	}

	//TODO: MAYAR: When updating the time, the data is always the last cell in the tuple vector!!!!!!!!
	public Vector<Object> UpdateTime(Vector<Object> tuple)
	{
		tuple.remove(tuple.size()-1);
		tuple.add(LocalDateTime.now());
		return tuple;
	}

	//to get a specific row from a page
	public Vector<Object> getTupleFromPage(int tupleIndx)
	{
		return PageElements.get(tupleIndx);
	}

	//method determines whether the new tuple will be inserted in this specific page
	//METHOD WORKS. IT HAS BEEN REVIEWED
	public boolean tupleToBePlacedInPage(int clusteringKeyIndx, Object clusteringKeyValue, String clusteringKeyType)
	{
		Object clusteringKeyLasttRow=PageElements.get(PageElements.size()-1).get(clusteringKeyIndx);

		if(clusteringKeyType.equals("java.lang.Integer"))
		{
			Integer intObj=(Integer)clusteringKeyLasttRow;
			Integer myintObj=(Integer) clusteringKeyValue;
			if(myintObj>intObj)
			{
				return false;
			}
		}
		else if(clusteringKeyType.equals("java.lang.String"))
		{
			String strObj=(String)clusteringKeyLasttRow;
			String mystrObj=(String) clusteringKeyValue;
			if(mystrObj.compareTo(strObj)>0)
			{
				return false;
			}
		}
		else if(clusteringKeyType.equals("java.lang.Double"))
		{
			Double dblObj=(Double)clusteringKeyLasttRow;
			Double mydblObj=(Double) clusteringKeyValue;
			if(mydblObj.compareTo(dblObj)>0)
			{
				return false;
			}
		}
		else if(clusteringKeyType.equals("java.util.Date"))
		{
			Date dateObj=(Date)clusteringKeyLasttRow;
			Date mydateObj=(Date) clusteringKeyValue;
			if(mydateObj.compareTo(dateObj)>0)
			{
				return false;
			}
		}
		else if(clusteringKeyType.equals("java.awt.Polygon"))
		{
			myPolygon polyObj=new myPolygon((Polygon)clusteringKeyLasttRow);
			myPolygon myPolyObj= new myPolygon((Polygon)clusteringKeyValue);
			if(myPolyObj.compareTo(polyObj)>0)
			{
				return false;
			}
		}
		return true;
	}

	public int binarySearch(int clusteringKeyIndx, Object clusteringKeyValue, String clusteringKeyType)
	{
		int first=0;
		int last=PageElements.size()-1;
		while(first<last)
		{
			int mid=(first+last)/2;
			Object temp=PageElements.get(mid).get(clusteringKeyIndx);

			if(clusteringKeyType.equals("java.lang.Integer"))
			{
				Integer intObj1=(Integer)temp;
				Integer myintObj=(Integer) clusteringKeyValue;
				if(myintObj<intObj1)
				{
					last=mid;
				}
				else
				{
					first=mid+1;
				}
			}
			else if(clusteringKeyType.equals("java.lang.String"))
			{
				String strObj1=(String)temp;
				String mystrObj=(String) clusteringKeyValue;
				if(mystrObj.compareTo(strObj1)<0)
				{
					last=mid;
				}
				else
				{
					first=mid+1;
				}
			}
			else if(clusteringKeyType.equals("java.lang.Double"))
			{
				Double dblObj1=(Double)temp;
				Double mydblObj=(Double) clusteringKeyValue;
				if(mydblObj.compareTo(dblObj1)<0)
				{
					last=mid;
				}
				else
				{
					first=mid+1;
				}
			}
			else if(clusteringKeyType.equals("java.util.Date"))
			{
				Date dateObj1=(Date)temp;
				Date mydateObj=(Date) clusteringKeyValue;
				if(mydateObj.compareTo(dateObj1)<0)
				{
					last=mid;
				}
				else
				{
					first=mid+1;
				}
			}
			else if(clusteringKeyType.equals("java.awt.Polygon"))
			{
				myPolygon polyObj1= new myPolygon((Polygon)temp);
				myPolygon mypolyObj=new myPolygon((Polygon) clusteringKeyValue);
				if(mypolyObj.compareTo(polyObj1)<0)
				{
					last=mid;
				}
				else
				{
					first=mid+1;
				}
			}
		}
		return first;
	}

	////METHOD WORKS. IT HAS BEEN REVIEWED
	public static void verifyTuple(String strTableName, Hashtable<String,Object> htblColNameValue, Hashtable<String, String>columnNameColumnType) throws DBAppException {
		//and checking if the user gave me a value for every column

		for (Map.Entry<String, String> mapElement :columnNameColumnType.entrySet()) {
			if(htblColNameValue.get(mapElement.getKey())==null)
			{
				//didn't receive a value for this specific column
				throw new DBAppException("Invalid tuple. Did not recieve values for every column");
			}
		}
		//check that column names in htblColNameValue actually exist and that the corresponding values respect the data type
		for(Map.Entry<String, Object> mapElement :htblColNameValue.entrySet())
		{
			//column name does not exist
			if(!columnNameColumnType.containsKey(mapElement.getKey()))
			{
				throw new DBAppException("Invalid column name.");
			}
			else
			{

				//determining type mismatch
				if(columnNameColumnType.get(mapElement.getKey()).equals("java.lang.Integer") && !(mapElement.getValue()instanceof Integer))
				{
					throw new DBAppException("Type mismatch. Cannot convert value to Integer.");
				}
				if(columnNameColumnType.get(mapElement.getKey()).equals("java.lang.String") && !(mapElement.getValue()instanceof String))
				{
					throw new DBAppException("Type mismatch. Cannot convert value to String.");
				}
				if(columnNameColumnType.get(mapElement.getKey()).equals("java.lang.Double") && !(mapElement.getValue()instanceof Double))
				{
					throw new DBAppException("Type mismatch. Cannot convert value to Double.");
				}
				if(columnNameColumnType.get(mapElement.getKey()).equals("java.lang.Boolean") && !(mapElement.getValue()instanceof Boolean))
				{
					throw new DBAppException("Type mismatch. Cannot convert value to Boolean.");
				}
				if(columnNameColumnType.get(mapElement.getKey()).equals("java.util.Date") && !(mapElement.getValue()instanceof Date))
				{
					throw new DBAppException("Type mismatch. Cannot convert value to Date.");
				}
				if(columnNameColumnType.get(mapElement.getKey()).equals("java.awt.Polygon") && !(mapElement.getValue()instanceof Polygon))
				{
					throw new DBAppException("Type mismatch. Cannot convert value to Polygon.");
				}
			}
		}
	}

	//used to create new tuple before inserting
	////METHOD WORKS. IT HAS BEEN REVIEWED
	public static Vector<Object> createNewTuple(ArrayList<String[]> metaDataForSpecificTable, Hashtable<String, Object> htblColNameValue)
	{
		Vector<Object> newTuple= new Vector<Object>();
		for(String[] column:metaDataForSpecificTable) {
			newTuple.add(htblColNameValue.get(column[1]));
		}
		return initializeTime(newTuple);
	}

	public String toString()
	{
		return this.PageElements + "";
	}

	/*
	 * TODO: FOR SAEED: delete from a page!
     MUST UPDATE: 
	 * count of the items in this page in this class
	 * the page array in TABLE class (if having to delete the page)
	 * the vector pageElements (due to deleted tuple)
	 */

	public void deleteByValue(Hashtable<Integer,Object> keyValue) {
		
		Set<Integer> indexedKeys= keyValue.keySet();
		
		for (int i = 0 ; i<PageElements.size();i++) {
			boolean match = true;
			Vector<Object> v = PageElements.get(i);
			for(int key: indexedKeys) {
				if(!(v.get(key).equals(keyValue.get(key)))){
				match = false;
				}
			}
			if(match) {
				PageElements.removeElementAt(i);
				i--;
				count--;
			}
			}
		}
}



