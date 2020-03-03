package DatabaseEngine;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;


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
	
	
	//to know max number of records per page.
	public int getN() {
		return N;
	}
	
	
	//get the unique page identifier. useful for deseralizing (because which page do u want boi)
	public int getID() {
		return pageID;
	}
	
	
	//get number of elements in page
	public int getElementsCount() {
		return this. count;
	}
	
	//get the vector of records
	public Vector<Vector> getPageElements()
	{
		return this.PageElements;
	}
	
	//TO SERIALIZE AND DESERIALIZE: check out utilities class.
	
	
	/*
	 * TODO: FOR ALI: insert into page!
	 MUST UPDATE: 
	 * count of the items in this page in this class
	 * the page array in TABLE class (if having to create new page)
	 * the vector pageElements (due to new created tuple)
	 */
	
	
	
	/*
	 * TODO: FOR SAEED: delete from a page!
     MUST UPDATE: 
	 * count of the items in this page in this class
	 * the page array in TABLE class (if having to delete the page)
	 * the vector pageElements (due to deleted tuple)
	 */

	//TODO for ALI AND SAEED: shifting and sorting

		
	//TODO: MAYAR AND ALI: any changes, update the timestamp!!
		public void updateTime() {
			//hint: LocalDateTime.now() gives current DateTime;	
		}
		
		
		
		//TODO: FOR ALI: verify tuple
		public boolean verifyTuple() {
			//according to metadata
			return false;
		}
	
	public static void main(String args[]) {
		try {
			
		}
		
		catch(Exception E) {
			System.out.println("for debugging, check main of PAGE class.");
		}
		
	}
	
	
}



