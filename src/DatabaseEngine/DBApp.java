package DatabaseEngine; //change to team name before submitting

import java.util.*;

import DatabaseEngine.BPlus.BPlusTree;
import DatabaseEngine.BPlus.BPointer;
import DatabaseEngine.R.RTree;
import javafx.util.Pair;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DBApp {

	public Hashtable<String, Hashtable<String, index>> indices; // table name -> column name -> tree (M2 code)

	public void init()  {
		Utilities.initializeMetaData();
		Utilities.initializeProperties();
		indices = Utilities.loadIndices();

		//TODO: add any other "initializing code" here!
	}

	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException {

		Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		//now the programmer may initialize a page to insert into it.
		//insert into it using Tuples object
	}

//	public static void main(String args[]) {
//		//CREATE TABLE TEST PASSED!
//
//		Hashtable t = new Hashtable<String, String>();
//		t.put("ID","java.lang.Integer");
//		t.put("name","java.lang.String");
//		t.put("isAdult","java.lang.Boolean");
//		t.put("deathdate","java.util.Date");
//		t.put("gpa","java.lang.Double");
//
//		DBApp d = new DBApp();
//
//
//	try {
//		 d.init();
//			d.createTable("dfawa", "name", t);
//	} catch (DBAppException e) {
//				System.out.println(e.getMessage());
//			e.printStackTrace();	}
//	}
		public static void main(String args[]) {
		//CREATE TABLE TEST PASSED!
		Hashtable table = new Hashtable<String, String>();
		table.put("name","java.lang.String");
		DBApp d = new DBApp();
		d.init();
		try {
			d.createTable("TestName2","name", table);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		Hashtable tuple = new Hashtable<String, Object>();
		
		tuple.clear();
		tuple.put("name", "Layla");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Hassan");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Farid");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Fawzeya");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Sara");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Saeed");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Tarek");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Mariam");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Nadine");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Rasheed");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Karim");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Jamal");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Hussein");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Malak");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Mohamed");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		
		tuple.clear();
		tuple.put("name", "Mayar");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Jamal");
		try {
			d.deleteFromTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Jameel");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Mayar");
		try {
			d.deleteFromTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Mayor");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Sara");
		try {
			d.deleteFromTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Sarag");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Layla");
		try {
			d.deleteFromTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Leila");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		try {
			d.createBTreeIndex("TestName2", "name");
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Jameel");
		try {
			d.deleteFromTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Jamal");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Mayor");
		try {
			d.deleteFromTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Mayar");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Sarag");
		try {
			d.deleteFromTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Sara");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Leila");
		try {
			d.deleteFromTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Layla");
		try {
			d.insertIntoTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		try {
			d.createBTreeIndex("TestName2", "name");
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Tarek");
		try {
			d.deleteFromTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		tuple.clear();
		tuple.put("name", "Farid");
		try {
			d.deleteFromTable("TestName2", tuple);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		Table obj=Utilities.deserializeTable("TestName2");


		for (int i = 0; i < obj.getPages().size(); i++) {
			Page p=Utilities.deserializePage(obj.getPages().get(i));
			System.out.println("id: " + p.getID());
			System.out.println(p);
			Utilities.serializePage(p);
		}

		Utilities.serializeTable(obj);
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
			//step 5:  use the value in the hash table called htblColNameValue to create a new tuple. This tuple will be inserted in step 8
			Vector newTuple = Page.createNewTuple(metaDataForSpecificTable, htblColNameValue);
			//step 6: Determine the page in which I should  do the insert(and determine the row number too) and retrieve it
			String[] clusteringKeyInfo = metaDataForSpecificTable.get(indexClusteringKey);
			String clusteringKeyType = clusteringKeyInfo[2];
			String[] temp=metaDataForSpecificTable.get(indexClusteringKey);
			int pageIndx=0;
			int rowNumber=0;
			//check if clustering key has an index
			if(temp[4].equals("True") && !clusteringKeyType.equals("java.awt.Polygon"))
			{
			//clustering key has a B+/R tree index so I should use that index to search for place	
			// load B+/R tree index
				index index= indices.get(strTableName).get(temp[1]);
				if(tableToInsertIn.getPages().isEmpty())
				{
					//in this case we know table is still empty. So I just need to create a new page
					//no need to use index here since table is empty
					tableToInsertIn.createNewPage();
					pageIndx=tableToInsertIn.getPages().get(0);
					rowNumber=0;
				}
				else
				{
					// since table is not empty, we must use the index
					BSet set;
					if(clusteringKeyType.equals("java.awt.Polygon"))
					{
						myPolygon poly= new myPolygon((java.awt.Polygon)htblColNameValue.get(clusteringKey));
						set=((RTree)index).search(poly, ">=");
					}
					else
					{
						set=((BPlusTree)index).search((Comparable)htblColNameValue.get(clusteringKey), ">=");
					}
					BPointer firstPtr=firstPtr=(BPointer)set.getMin();
				
					if(firstPtr==null)
					{
						//inserting last tuple in table
						Page p=Utilities.deserializePage(tableToInsertIn.getPages().get(tableToInsertIn.getPages().size()-1));
						// last page is full must create new one
						if(p.getElementsCount()==p.getN())
						{
							tableToInsertIn.createNewPage();
							pageIndx=tableToInsertIn.getPages().get(tableToInsertIn.getPages().size()-1);
							rowNumber=0;
						}
						else
						{
							//last page not full. Insert in it
							pageIndx=tableToInsertIn.getPages().get(tableToInsertIn.getPages().size()-1);
							rowNumber=p.getElementsCount();
						}
						Utilities.serializePage(p);
					}
					else
					{
						//might insert in page before if there's space and the firstPtr is 0
						Page p=tableToInsertIn.getPrevPage(firstPtr.getPage());
						if(p!=null && p.getElementsCount()<p.getN() && firstPtr.getOffset()==0)
						{
							pageIndx=tableToInsertIn.getPages().get(tableToInsertIn.getPageIndx(p));
							rowNumber=p.getElementsCount();
						}
						else
						{
							// normal case
							pageIndx=firstPtr.getPage();
							rowNumber=firstPtr.getOffset();
						}
					}
				}
			}
			else
			{
				//no clustering index
				//using binary search to find page index and row number for insertion
				int[] toInsertIn=tableToInsertIn.determinePageNeededForInsertUsingBinarySearch(indexClusteringKey, htblColNameValue.get(clusteringKey), clusteringKeyType);
				pageIndx=toInsertIn[0];
				rowNumber=toInsertIn[1];
			}
			//System.out.println(pageIndx + " "+ rowNumber);
			//step 7: inserting in B+ tree and R tree indeces (if any exist).
			//System.out.println("checking if I need to insert in a B+ tree index");
			for (int i = 0; i <metaDataForSpecificTable.size() ; i++) {
				//System.out.println("Retrieving metadata for column " + i);
				temp=metaDataForSpecificTable.get(i);
				//System.out.println("Metadata: " + Arrays.toString(temp));
				
				if(temp[4].equals("True") && !temp[2].equals("java.awt.Polygon"))
				{
					//System.out.println("B+ tree exists for column " + temp[1]);
					BPlusTree tree= (BPlusTree) indices.get(strTableName).get(temp[1]);
					//System.out.println("tree fetched");
					if(temp[2].equals("java.lang.Integer"))
					{
						//System.out.println("Tree is of type integer");
						tree.insert((Integer)htblColNameValue.get(temp[1]), new BPointer(pageIndx, rowNumber), true);					}
					else if(temp[2].equals("java.lang.String"))
					{
						//System.out.println("Tree is of type string");
						tree.insert((String)htblColNameValue.get(temp[1]), new BPointer(pageIndx, rowNumber), true);					}
					else if(temp[2].equals("java.lang.Double"))
					{
						//System.out.println("Tree is of type double");
						tree.insert((Double)htblColNameValue.get(temp[1]), new BPointer(pageIndx, rowNumber), true);					}
					else if(temp[2].equals("java.lang.Boolean"))
					{
						//System.out.println("Tree is of type boolean");
						tree.insert((Boolean)htblColNameValue.get(temp[1]), new BPointer(pageIndx, rowNumber), true);					}
					else if(temp[2].equals("java.util.Date"))
					{
						//System.out.println("Tree is of type date");
						tree.insert((Date)htblColNameValue.get(temp[1]), new BPointer(pageIndx, rowNumber), true);
					}
					//System.out.println("value: " + htblColNameValue.get(temp[1]) + " in page " + ans.get(0) + " in row " + ans.get(1));
					Utilities.serializeBPT(tree);
					//System.out.println("Serializing tree");
				}
				else if (temp[4].equals("True"))
				{
					//Rtree
					// fetch tree
					RTree tree= (RTree) indices.get(strTableName).get(temp[1]);
					//insert in tree"java.awt.Polygon"
					tree.insert(new myPolygon((java.awt.Polygon)htblColNameValue.get(temp[1])), new BPointer(pageIndx, rowNumber), true);
					//serialize
					Utilities.serializeRTree(tree);
				}
					
					
			}
			
			//Step 8: Insert actual tuple
			tableToInsertIn.insertRegularCase(newTuple, pageIndx, rowNumber, indexClusteringKey, htblColNameValue.get(clusteringKey), clusteringKeyType);
			//Step 9:serialize table again
			Utilities.serializeTable(tableToInsertIn);
			this.indices = Utilities.loadIndices();
		}
		else
		{
			throw new DBAppException("Table " + strTableName + " does not exist. Cannot insert in a nonexistant table.");
		}
	}

	
	//---------------------------------------------------------------------UPDATE METHOD----------------------------------------------------------------------
	
	
	//updates using binary search
	public void updateBS(String strTableName, String strClusteringKey, String[] cluster, Hashtable<String, Object> htblColNameValue) throws DBAppException
	{
		
		//System.out.println("Entering BS");
		
		//figure out the index of the clustering column
		int clusterIdx = Utilities.returnIndex(strTableName, cluster[0]);

		String clusterType = cluster[1];

		Comparable clusterKey;
		//if clustering key is of type integer
		if (clusterType.equals("java.lang.Integer"))
			clusterKey = Integer.parseInt(strClusteringKey);
		else if (clusterType.equals("java.lang.Double"))
			clusterKey = Double.parseDouble(strClusteringKey);
		
		else if (clusterType.equals("java.util.Date"))
			try {
				clusterKey = new SimpleDateFormat("YYYY-MM-DD").parse(strClusteringKey);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}
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

		

		Table t = Utilities.deserializeTable(strTableName);

		Vector<Integer> pagesID = t.getPages();
		
		boolean flag = false; //flag that checks if at least one page is visited

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
				flag = true;
				Hashtable<String, index> ind = this.indices.get(strTableName);
				Utilities.binarySearchUpdate(records, 0, records.size() - 1, clusterIdx, clusterKey, strTableName, htblColNameValue, ind, page.getID(), false);

			}

			Utilities.serializePage(page);
		}

		Utilities.serializeTable(t);
		
		if(!flag)
			throw new DBAppException("No record with clustering key " + strClusteringKey + " exists to update");

	}
	
	public void updateIndex(String strTableName, String strClusteringKey, String[] cluster,  Hashtable<String, Object> htblColNameValue) throws DBAppException
	{
		
		//System.out.println("Entering indexing");
		
		//figure out the index of the clustering column
		int clusterIdx = Utilities.returnIndex(strTableName, cluster[0]);

		String clusterType = cluster[1];
		
		//get the HT of all indices for the table
		Hashtable<String, index> ind = this.indices.get(strTableName);
		
		BSet<BPointer> records;
		
		if (clusterType.equals("java.lang.Integer"))
		{
			Integer clusterKey = Integer.parseInt(strClusteringKey);
			BPlusTree<Integer> clusterTree = (BPlusTree<Integer>)ind.get(cluster[0]);
			records = clusterTree.search(clusterKey, "=");
		}
		else if (clusterType.equals("java.lang.Double"))
		{
			Double clusterKey = Double.parseDouble(strClusteringKey);
			BPlusTree<Double> clusterTree = (BPlusTree<Double>)ind.get(cluster[0]);
			records = clusterTree.search(clusterKey, "=");
		}
		else if (clusterType.equals("java.util.Date"))
			try {
				Date clusterKey = new SimpleDateFormat("YYYY-MM-DD").parse(strClusteringKey);
				BPlusTree<Date> clusterTree = (BPlusTree<Date>)ind.get(cluster[0]);
				records = clusterTree.search(clusterKey, "=");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}
		else if (clusterType.equals("java.lang.String"))
		{
			String clusterKey = strClusteringKey;
			BPlusTree<String> clusterTree = (BPlusTree<String>)ind.get(cluster[0]);
			records = clusterTree.search(clusterKey, "=");
		}
			else if(clusterType.equals("java.awt.Polygon"))
			{
				myPolygon clusterKey = myPolygon.parsePolygon(strClusteringKey);
				RTree clusterTree = (RTree)ind.get(cluster[0]);
				records = clusterTree.search(clusterKey, "=");
				//System.out.println(records);
			}
		else if (clusterType.equals("java.lang.Boolean")) 
		{
			Boolean clusterKey = Boolean.parseBoolean(strClusteringKey);
			BPlusTree<Boolean> clusterTree = (BPlusTree<Boolean>)ind.get(cluster[0]);
			records = clusterTree.search(clusterKey, "=");
		} 
		else 
		{
			System.out.println("Invalid cluster data type detected in updateTable() method. \n"
					+ "Make sure " + strTableName + " table's metadata for the clustering column is inputted correctly");
			return;
		}
		
		if(records.size()==0)
		{
			throw new DBAppException("No record with clustering key " + strClusteringKey + " exists to update");
		}
		
		Hashtable<Integer, Vector<Integer>> collectedOffset = Utilities.collectOffsets(records);
		
		Set<Integer> pageIDs = collectedOffset.keySet();
		
		
		for(int id : pageIDs)
		{
			//System.out.println("Page " + id);
			Page page = Utilities.deserializePage(id);
			Vector<Vector> tuples = page.getPageElements();
			
			Utilities.indexSearchUpdate(strTableName, tuples, collectedOffset.get(id), htblColNameValue, ind, id);
			
			Utilities.serializePage(page);
		}
			
			
		


	}
	
	
	
	public void updateTable(String strTableName, String strClusteringKey,
							Hashtable<String, Object> htblColNameValue) throws DBAppException 
	{
		//check if table exists, all columns exits and if they do check if the type of object matches
		boolean valid = Utilities.updateChecker(strTableName, htblColNameValue);


		//either table does not exist, column name does not exist or type mismatch for data values
		if (!valid) return;


		// figure out the column name, type of clustering key and indexing boolean,
		// respectively
		String[] cluster = Utilities.returnClustering(strTableName);
		
		if(cluster == null) return; //an exception is caught in returnClustering()
		
		Boolean indexed = Boolean.parseBoolean(cluster[2]);

		// if the clustering column is not indexed, use binary search
		if (!indexed)
			this.updateBS(strTableName, strClusteringKey, cluster, htblColNameValue);
		 else 
			this.updateIndex(strTableName, strClusteringKey, cluster, htblColNameValue);
		
			
	}

//-----------------------------------------------------------------END OF UPDATE------------------------------------------------------------------------

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Table t = Utilities.deserializeTable(strTableName);
		Hashtable<String, Hashtable<String, index>> indices = Utilities.loadIndices();
//		t.delete(htblColNameValue);
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[htblColNameValue.size()];
//		SQLTerm[] arrSQLTerms;
//		arrSQLTerms = new SQLTerm[2];
//		arrSQLTerms[0]._strTableName = "Student";
//		arrSQLTerms[0]._strColumnName= "name";
//		arrSQLTerms[0]._strOperator = "=";
//		arrSQLTerms[0]._objValue = "John Noor"; 
		Set<String> keys = htblColNameValue.keySet();
		int z = 0;
		for(String key : keys) { 
			if(htblColNameValue.get(key) instanceof java.awt.Polygon)
			{
				SQLTerm term = new SQLTerm();
				term._strTableName = strTableName;
				term._strColumnName = key;
				term._strOperator = "=";
				term._objValue = new myPolygon((java.awt.Polygon)(htblColNameValue.get(key)));
				arrSQLTerms[z] = term;
				z++;	
			}else {
			SQLTerm term = new SQLTerm();
			term._strTableName = strTableName;
			term._strColumnName = key;
			term._strOperator = "=";
			term._objValue = htblColNameValue.get(key);
			arrSQLTerms[z] = term;
			z++;
			}
			}
		String[]strarrOperators = new String[arrSQLTerms.length-1];
		for(int i = 0; i< strarrOperators.length;i++) {
			strarrOperators[i] = "AND"; 
		}
	BSet<BPointer> pointers =	Utilities.selectPointers(indices, arrSQLTerms, strarrOperators);
	Iterator<Vector<Object>> rows = Utilities.getPointerRecords(pointers);
	if(pointers.size()==0) {
		throw new DBAppException("No matching records were found");
	}
	else
	if(indices.containsKey(strTableName)) {
	
		ArrayList<String[]> metaData = Utilities.readMetaDataForSpecificTable(strTableName);
		Hashtable<String, index> columnTreeIndices = indices.get(strTableName);
		Hashtable<String,ArrayList> columnIndexWithinTable = new Hashtable<String, ArrayList>();
		for(int i = 0; i <metaData.size(); i++) {
			for(String key: columnTreeIndices.keySet()) {
				ArrayList temp = new ArrayList();
				if(((String)metaData.get(i)[1]).equals(key)) {
					temp.add(0,i);
					temp.add(1, metaData.get(i)[2]);
					columnIndexWithinTable.put(key, temp);
					break;
				}
			}
		}
		
		ArrayList<BPointer> pointersArray = new ArrayList<BPointer>();
		for(BPointer point: pointers) {
			pointersArray.add(point);
			}
		int itr = 0;
		while(rows.hasNext()) {
			BPointer p = pointersArray.get(itr++);
			Vector row = rows.next();
			
			for(String key: columnTreeIndices.keySet()) {
		Comparable value=	(Comparable) row.get(((int)columnIndexWithinTable.get(key).get(0)));
		String type = (String)columnIndexWithinTable.get(key).get(1);
		if(!(type.equals("java.awt.Polygon"))) {
			((BPlusTree)	columnTreeIndices.get(key)).delete(value,p,type,true);
			
		}
		else {
			// Call the Rtree's delete
			((RTree)	columnTreeIndices.get(key)).delete((myPolygon)value,p,type,true);
		}
		}
			t.delete(p.getPage(), p.getOffset());
			
		for(int i = itr;i<pointersArray.size();i++) {
			BPointer temp = pointersArray.get(i);
			if (temp.compareTo(p) >= 0 && p.getPage() == temp.getPage())
			{
				temp.setOffset(temp.getOffset() - 1);			
				pointersArray.set(i, temp);
			}
				
		}
		
	}	}
	else {
		ArrayList<BPointer> pointersArray = new ArrayList<BPointer>();
		for(BPointer point: pointers) {
			pointersArray.add(point);
			}
		for(int itr =0; itr <pointersArray.size();itr++) {
			BPointer temp = pointersArray.get(itr);
			for(int i = itr+1;i<pointersArray.size();i++) {
				BPointer p = pointersArray.get(i);
				if (p.compareTo(temp) >= 0 && p.getPage() == temp.getPage())
				{
				p.setOffset(p.getOffset() - 1);			
				pointersArray.set(i, p);
				}
				}
		}
	}
		this.indices = Utilities.loadIndices();
	}
	//----------------------------------M2------------------------------------------
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException{
		//----=not enough operators=-----
			if (strarrOperators.length != arrSQLTerms.length - 1)
				throw new DBAppException("the number of operators is incorrect!");

		BSet<BPointer> resultPointers = null; //final pointers of the select statement
		int i = -1; //strarrOperator index

		for(SQLTerm cur: arrSQLTerms){ //for each SQLTerm

			//------------------------------------Integrity checks------------------------------------

			//--current term is complete-
				if (!Utilities.validTerm(cur)) throw new DBAppException("Incomplete SQLTerm!");

			//--------table exists-------
				Table cur_table = Utilities.deserializeTable(cur._strTableName);
				if (cur_table == null) throw new DBAppException("Table not found!");

			//---extract table metadata--
				ArrayList<String[]> metaData = Utilities.readMetaDataForSpecificTable(cur._strTableName);
				if (metaData == null) throw new DBAppException("Cannot fetch metadata!");

			//-------column exists-------
				Pair<String[],Integer> check = Utilities.getColumnFromMetadata(cur._strColumnName,metaData);
				String[] colInfo = check.getKey();//column metadata
				Integer colnum = check.getValue(); //index of that column
				if (colInfo == null) throw new DBAppException("Attribute not found!");

			//-------correct type--------
				Class colType = Utilities.correctType(colInfo[2],cur._objValue);
				if (colType == null) throw new DBAppException("term value is incompatible with column type!");

			//------correct operator-----
				if (!Utilities.allowedOperator(cur._strOperator)) throw new DBAppException("Unrecognized operator!");;

			//------indexed column?------
				Boolean Indexed = colInfo[4].charAt(0) == 'T';

			//-------retrieve Index------
				index tree = null;
				if (indices != null && indices.get(cur._strTableName).containsKey(cur._strColumnName)) //find tree
					tree = indices.get(cur._strTableName).get(cur._strColumnName);

				if (Indexed && tree == null)
					throw new DBAppException("Could not find index!");

			//------------------------------------Execution------------------------------------
				BSet<BPointer> queryResult; //results of the the current query

				if (Indexed){ //binary search in tree
					queryResult = Utilities.indexedQuery(colType,tree,cur);
				}
				else { //no index, search in records
					queryResult = Utilities.recordQuery(cur,colInfo[3].charAt(0) == 'T',cur_table,colnum,colType);
				}

			//-----------perform set operation-----------
			if (i == -1){  //first term (no operator)
				resultPointers = Utilities.setOperation(resultPointers,queryResult,null);
				i++;
			}
			else {
				resultPointers = Utilities.setOperation(resultPointers,queryResult,strarrOperators[i++].toUpperCase());
			}



		} //repeat for all SQL terms
		
		return Utilities.getPointerRecords(resultPointers); //return an iterator containing the records extracted from resultPointers
	}
	
	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException{
		//name of index (DELETE IT)
		String name=strTableName+strColName;
		//System.out.println("Name: " + name);
		//load metadata of table
		//System.out.println("Fetching metadata");
		ArrayList<String[]> metaData=Utilities.readMetaDataForSpecificTable(strTableName);
		//System.out.println("MetaData: " + metaData);
		if(metaData.isEmpty())
		{
			//table doesn't exist in meta data
			throw new DBAppException("Table " + strTableName + " has not been initialized. Cannot create index for uninitialized table.");
		}
		else
		{
			//System.out.println("Table exists");
			String[] metaDataForIndexedColumn=null;
			int locationOfIndexedColumn=0;
			for (int i = 0; i < metaData.size(); i++) {
				String[] temp=metaData.get(i);
				if(temp[1].equals(strColName))
				{
					String type=temp[2];
					//check that column is not of type polygon
					if(type.equals("java.awt.Polygon"))
					{
						throw new DBAppException("Cannot create B+ tree index on type polygon");
					}
					if(temp[4].equals("True"))
					{
						throw new DBAppException("B+ tree index already exists for this column");
					}
					locationOfIndexedColumn=i;
					metaDataForIndexedColumn=temp;
					//System.out.println("metaDataForIndexedColumn" + Arrays.toString(metaDataForIndexedColumn));
					break;
					
				}
			}
			if(metaDataForIndexedColumn==null)
			{
				//column name doesn't exist in meta data
				throw new DBAppException("Column " + strColName +" in table " + strTableName + " does not exist. Cannot create index for nonexistent column.");
			}
			else
			{
				//System.out.println("Column exists");
				//all is well. I can now create the appropriate B+ tree 
				//determining column type to create appropriate BTree
				//System.out.println("Tree should be of type " + type);
				//creating appropriate Btree
				//System.out.println("creating Btree");
				String type=metaDataForIndexedColumn[2];
				//check that column is not of type polygon
				if(type.equals("java.awt.Polygon"))
				{
					throw new DBAppException("Cannot create B+ tree index on type polygon");
				}
				BPlusTree tree=null;
				if(type.equals("java.lang.Integer"))
				{
					tree= new BPlusTree<Integer>(strTableName, strColName);
					//System.out.println("tree of type integer created" );
				}
				else if(type.equals("java.lang.String"))
				{
					tree= new BPlusTree<String>(strTableName, strColName);
					//System.out.println("tree of type string created" );
				}
				else if(type.equals("java.lang.Double"))
				{
					tree= new BPlusTree<Double>(strTableName, strColName);
					//System.out.println("tree of type double created" );
				}
				else if(type.equals("java.lang.Boolean"))
				{
					tree= new BPlusTree<Boolean>(strTableName, strColName);
					//System.out.println("tree of type boolean created" );
				}
				else if(type.equals("java.util.Date"))
				{
					tree= new BPlusTree<java.util.Date>(strTableName, strColName);
					//System.out.println("tree of type date created" );
				}
				//System.out.println("Deserializing table");
				Table t= Utilities.deserializeTable(strTableName);
				for (int i = 0; i < t.getPages().size(); i++) {
					//System.out.println("deserializing page");
					Page p= Utilities.deserializePage(t.getPages().get(i));
					//System.out.println("Page: " + p);
					for (int j = 0; j < p.getPageElements().size(); j++) {
						Vector<Object> tuple=p.getTupleFromPage(j);
						Object value=tuple.get(locationOfIndexedColumn);
						if(type.equals("java.lang.Integer"))
						{
							//System.out.println("Inserting an integer");
							tree.insert((int)value, new BPointer(t.getPages().get(i), j), false);
						}
						else if(type.equals("java.lang.String"))
						{
							//System.out.println("Inserting a string");
							tree.insert((String)value, new BPointer(t.getPages().get(i), j), false);
						}
						else if(type.equals("java.lang.Double"))
						{
							//System.out.println("Inserting a double");
							tree.insert((Double)value, new BPointer(t.getPages().get(i), j), false);
						}
						else if(type.equals("java.lang.Boolean"))
						{
							//System.out.println("Inserting a boolean");
							tree.insert((Boolean)value, new BPointer(t.getPages().get(i), j), false);
						}
						else if(type.equals("java.util.Date"))
						{
							//System.out.println("Inserting a date");
							tree.insert((Date)value, new BPointer(t.getPages().get(i), j), false);
						}
						//System.out.println("value : " + value + " in page " + t.getPages().get(i) + " and row " + j);
					}
					Utilities.serializePage(p);
					//System.out.println("serialized page");
				}
				Utilities.serializeTable(t);
				//System.out.println("serializing table");
				Utilities.serializeBPT(tree);
				//System.out.println("Serializing tree");
				//modify metaData
				Utilities.updateMetaData(strTableName, strColName);
				//System.out.println("Metadata modified");
				//add to hashtable
				indices = Utilities.loadIndices();
			}
		}
		
	}

	public void createRTreeIndex(String strTableName, String strColName) throws DBAppException{
		//load metadata of table
		//System.out.println("Fetching metadata");
		ArrayList<String[]> metaData=Utilities.readMetaDataForSpecificTable(strTableName);
		//System.out.println("MetaData: " + metaData);
		if(metaData.isEmpty())
		{
			//table doesn't exist in meta data
			throw new DBAppException("Table " + strTableName + " has not been initialized. Cannot create index for uninitialized table.");
		}
		else
		{
			//System.out.println("Table exists");
			String[] metaDataForIndexedColumn=null;
			int locationOfIndexedColumn=0;
			for (int i = 0; i < metaData.size(); i++) {
				String[] temp=metaData.get(i);
				if(temp[1].equals(strColName))
				{
					String type=temp[2];
					//check that column is not of type polygon
					if(!type.equals("java.awt.Polygon"))
					{
						throw new DBAppException("Cannot create R tree index on type " + type);
					}
					if(temp[4].equals("True"))
					{
						throw new DBAppException("R tree index already exists for this column");
					}
					locationOfIndexedColumn=i;
					metaDataForIndexedColumn=temp;
					//System.out.println("metaDataForIndexedColumn" + Arrays.toString(metaDataForIndexedColumn));
					break;

				}
			}
			if(metaDataForIndexedColumn==null)
			{
				//column name doesn't exist in meta data
				throw new DBAppException("Column " + strColName +" in table " + strTableName + " does not exist. Cannot create index for nonexistent column.");
			}
			else
			{
				//System.out.println("Column exists");
				//all is well. I can now create the appropriate B+ tree
				//determining column type to create appropriate BTree
				//System.out.println("Tree should be of type " + type);
				//creating appropriate Rtree
				//System.out.println("creating Rtree");
				RTree tree= new RTree(strTableName, strColName);

				//System.out.println("Deserializing table");
				Table t= Utilities.deserializeTable(strTableName);
				for (int i = 0; i < t.getPages().size(); i++) {
					//System.out.println("deserializing page");
					Page p= Utilities.deserializePage(t.getPages().get(i));
					//System.out.println("Page: " + p);
					for (int j = 0; j < p.getPageElements().size(); j++) {
						Vector<Object> tuple=p.getTupleFromPage(j);
						Object value=tuple.get(locationOfIndexedColumn);

						tree.insert((myPolygon) value, new BPointer(t.getPages().get(i), j), false);
					}
					Utilities.serializePage(p);
					//System.out.println("serialized page");
				}
				Utilities.serializeTable(t);
				//System.out.println("serializing table");
				Utilities.serializeRTree(tree);
				//System.out.println("Serializing tree");
				//modify metaData
				Utilities.updateMetaData(strTableName, strColName);
				//System.out.println("Metadata modified");
				//add to hashtable
				indices = Utilities.loadIndices();
			}
		}
	}
	

}

