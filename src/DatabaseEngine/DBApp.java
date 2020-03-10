package DatabaseEngine; //change to team name before submitting

import java.sql.Date;
import java.util.Hashtable;
import java.util.Vector;

import javafx.util.Pair;

import java.util.*;


public class DBApp {

	private Hashtable<String, Hashtable<String, index>> indices; // table name -> column name -> tree (M2 code)

	public void init() throws DBAppException {
		Utilities.initializeMetaData();
		Utilities.initializeProperties();
		indices = Utilities.loadIndices();


		//TODO: add any other "initializing code" here!
	}

	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException {

		Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		//now the programmer may initialize a page to insert into it.
		//insert into it using Tuples object
		Utilities.serializeTable(t);
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

      
	try {
		 d.init();
			d.createTable("dfawa", "name", t);
	} catch (DBAppException e) {
				System.out.println(e.getMessage());
			e.printStackTrace();	}
	}


	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		//Step 0: Load table object
		Table tableToInsertIn = Utilities.deserializeTable(strTableName);
		if (tableToInsertIn != null) {
			//Step 1: load meta data for specific table
			ArrayList<String[]> metaDataForSpecificTable = Utilities.readMetaDataForSpecificTable(strTableName);
			//Step 2: store column names and corresponding types of metaDataForSpecificTable in hash table to make life simpler
			Hashtable<String, String> columnNameColumnType = Utilities.extractNameAndTypeFromMeta(metaDataForSpecificTable);
			//Step 3: Determine clustering of table and its index in the tuple vector
			String[] tempArray = null;
			tempArray = Utilities.determineValueAndIndexOfClusteringKey(metaDataForSpecificTable);
			String clusteringKey = tempArray[0];
			int indexClusteringKey = Integer.parseInt(tempArray[1]);
			//step 4: Verify that tuple is valid before attempting to insert it
			Page.verifyTuple(strTableName, htblColNameValue, columnNameColumnType);
			//step 5:  use the value in the hash table called htblColNameValue to create a new tuple. This tuple will be inserted in step 8 or 9
			Vector newTuple = Page.createNewTuple(metaDataForSpecificTable, htblColNameValue);
			//step 6: Determine the page in which I should  do the insert and retrieve it
			String[] clusteringKeyInfo = metaDataForSpecificTable.get(indexClusteringKey);
			String clusteringKeyType = clusteringKeyInfo[2];
			Page toInsertIn = tableToInsertIn.determinePageNeededForInsert(indexClusteringKey, htblColNameValue.get(clusteringKey), clusteringKeyType, newTuple);
			//Step 7: *SPECIAL CASE OF INSERT* if it was determined in step 6 that the tuple to be inserted is the last in the ENTIRE table
			if (toInsertIn == null) {
				tableToInsertIn.insertSpecialCase(newTuple);
			}
			//Step 8: *REGULAR CASE OF INSERT*
			else if (toInsertIn.getElementsCount() != 0) {
				tableToInsertIn.insertRegularCase(newTuple, toInsertIn, indexClusteringKey, htblColNameValue.get(clusteringKey), clusteringKeyType);
			}
		}
		//Step 9:serialize page again
		Utilities.serializeTable(tableToInsertIn);
	}

	//---------------------------------------------------------------------UPDATE METHOD--------------------------------------------------------------------

	//Mimi's part: update the records :)


	//TODO: move to the TABLE class ya bent
	//TODO: are many attributes updated or just one at a time?
	//If only one at a time, simplify updateChecker (inc. Hashtable implementation)

	public void updateTable(String strTableName, String strClusteringKey,
							Hashtable<String, Object> htblColNameValue) throws DBAppException {
		//check if table exists, all columns exits and if they do check if the type of object matches
		boolean valid = Utilities.updateChecker(strTableName, htblColNameValue);


		//either table does not exist, column name does not exist or type mismatch for data values
		if (!valid) return;

		else {
			//TODO: search for the record with index and update the data values

			//figure out the column name and type of clustering key, respectively
			Pair<String, String> cluster = Utilities.returnClustering(strTableName);

			//figure out the index of the clustering column
			int clusterIdx = Utilities.returnIndex(strTableName, cluster.getKey());

			String clusterType = cluster.getValue();

			Comparable clusterKey;
			//if clustering key is of type integer
			if (clusterType.equals("java.lang.Integer"))
				clusterKey = Integer.parseInt(strClusteringKey);
			else if (clusterType.equals("java.lang.Double"))
				clusterKey = Double.parseDouble(strClusteringKey);
			else if (clusterType.equals("java.util.Date"))
				clusterKey = Date.valueOf(strClusteringKey);
			else if (clusterType.equals("java.lang.String"))
				clusterKey = strClusteringKey;
  			else if(clusterType.equals("java.awt.Polygon"))
  				clusterKey = myPolygon.parsePolygon(strClusteringKey);
			else if (clusterType.equals("java.lang.Boolean")) {
				clusterKey = Boolean.parseBoolean(strClusteringKey);
			} else {
				System.out.println("Invalid cluster data type detected in updateTable() method. \n"
						+ "Make sure " + strTableName + " table's metadata for the clustering column is inputted correctly");
				return;
			}

			//TODO: use binary search for pages?

			Table t = Utilities.deserializeTable(strTableName);

			Vector<Integer> pagesID = t.getPages();


			for (int i = 0; i < pagesID.size(); i++) {
				Page page = Utilities.deserializePage(pagesID.get(i));
				Vector<Vector> records = page.getPageElements();

				//if the first element in the page is greater than the clustering key => element will not be in the page
				//OR
				//if the last element is less than the clustering key => element will not be in the page
				//then binary search through the page to find the record to update
				//DeMorgan's law is beautiful
				if (((Comparable) records.firstElement().get(clusterIdx)).compareTo(clusterKey) <= 0 &&
						((Comparable) records.lastElement().get(clusterIdx)).compareTo(clusterKey) >= 0) //ignore this stupid warning, Ali should check clustering entered implements comparable
				{
					Utilities.binarySearchUpdate(records, 0, records.firstElement().size() - 1, clusterIdx, clusterKey, strTableName, htblColNameValue);

				}

				Utilities.serializePage(page);
			}

			Utilities.serializeTable(t);

		}

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Table t = Utilities.deserializeTable(strTableName);
		t.delete(htblColNameValue);

	}

}

