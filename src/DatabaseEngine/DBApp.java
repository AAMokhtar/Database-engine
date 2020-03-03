package DatabaseEngine; //change to team name before submitting

import java.awt.Dimension;
import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import javafx.util.Pair;

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
		
		Hashtable t = new Hashtable<String, String>();
		t.put("ID","java.lang.Integer");
		t.put("name","java.lang.String");
		t.put("isAdult","java.lang.Boolean");
		t.put("deathdate","java.util.Date");
		t.put("gpa","java.lang.Double");
		
		DBApp d = new DBApp();

//       d.init();
		try {
			d.createTable("Table Name", "name", t);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();	}
	}
    
		
		//TODO: FOR ALL Y'ALL: PLEASE KEEP TESTS IN COMMENTS!! 
		
	

    
//Ali's part:

    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{


    }
    //Mayar's part:
    //---------------------------------------------------------------------UPDATE METHOD--------------------------------------------------------------------
	
  	//Mimi's part: update the records :)
  	
  	
  	
  	
  	
  	//TODO: move to the TABLE class ya bent
  	//TODO: are many attributes updated or just one at a time?
  	//If only one at a time, simplify updateChecker (inc. Hashtable implementation)
  	
  	public void updateTable(String strTableName, String strClusteringKey, 
  			                       Hashtable<String,Object> htblColNameValue ) throws DBAppException
  	{
  		//check if table exists, all columns exits and if they do check if the type of object matches
  		boolean valid = Utilities.updateChecker(strTableName, htblColNameValue);
  		
  		
  		//either table does not exist, column name does not exist or type mismatch for data values
  		if(!valid) return;
  		
  		else
  		{
  			//TODO: search for the record with index and update the data values
  			
  			//figure out the column name and type of clustering key, respectively
  			Pair<String,String> cluster = Utilities.returnClustering(strTableName);
  			
  			//figure out the index of the clustering column
  			int clusterIdx = Utilities.returnIndex(strTableName, cluster.getKey());
  			
  			String clusterType = cluster.getValue();
  			
  			Comparable clusterKey;
  			//if clustering key is of type integer
  			if(clusterType.equals("java.lang.Integer"))
  				clusterKey = Integer.parseInt(strClusteringKey);
  			else if(clusterType.equals("java.lang.Double"))
  				clusterKey = Double.parseDouble(strClusteringKey);
  			else if(clusterType.equals("java.util.Date"))
  				clusterKey = Date.valueOf(strClusteringKey);
  			else if(clusterType.equals("java.lang.String"))
  				clusterKey = strClusteringKey;
  			//TODO: uncomment this part when updating the code
//  			else if(clusterType.equals("java.awt.Polygon"))
//  				clusterKey = polygonParse(strClusteringKey);
  			//TODO: are we sure bools cannot be clusters?
  			else if(clusterType.equals("java.lang.Boolean"))
  			{
  				System.out.println("Boolean clustering data type detected in updateTable() method");
  				return;
  			}
  			else
  			{
  				System.out.println("Invalid cluster data type detected in updateTable() method. \n"
  						+ "Make sure " + strTableName + " table's metadata for the clustering column is inputted correctly");
  				return;
  			}
  				
  			//TODO: use binary search for pages?
  			
  			Table t = Utilities.deserializeTable(strTableName);
  			
  			Vector<Integer> pagesID = t.getPages();
  			
  			//TODO: ask basant about this
  			boolean finito = false; //flag that identifies when to break from loop
  			
  			for(int i=0;i<pagesID.size() && !finito;i++)
  			{
  					Page page = Utilities.deserializePage(pagesID.get(i));
  					Vector<Vector> records = page.getPageElements();
  					
  					//if the first element in the page is greater than the clustering key => element will not be in the page
  					//OR
  					//if the last element is less than the clustering key => element will not be in the page
  					//then binary search through the page to find the record to update
  					//DeMorgan's law is beautiful
  					if(((Comparable)records.firstElement().get(clusterIdx)).compareTo(clusterKey)<=0 && 
  							((Comparable)records.lastElement().get(clusterIdx)).compareTo(clusterKey)>=0) //ignore this stupid warning, Ali should check clustering entered implements comparable
  					{
  						Utilities.binarySearchUpdate(records, 0, records.firstElement().size()-1, clusterIdx, clusterKey, strTableName, htblColNameValue);
  						
  					}
  					//if the page encountered has its first element clustering value greater than the key => abort
  					//we are no longer going to find the clustering key in this page or the following
  					else if(((Comparable)records.firstElement().get(clusterIdx)).compareTo(clusterKey)>0)
  						finito = true;
  					
  					Utilities.serializePage(page);
  			}
  			
  			Utilities.serializeTable(t);
  	
  		}
  		
  	}
  	

//Saeed's part:

    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{

    }

    
}
