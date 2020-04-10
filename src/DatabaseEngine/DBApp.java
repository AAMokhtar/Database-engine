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
		//table.put("ID","java.lang.Integer");
		//table.put("name","java.lang.String");
		//table.put("isAdult","java.lang.Boolean");
		//table.put("nationality","java.lang.String");
		//table.put("birthdate","java.util.Date");
		//table.put("gpa","java.lang.Double");
		table.put("shape", "java.awt.Polygon");
		DBApp d = new DBApp();
		d.init();
		try {
			d.createTable("Test1","shape", table);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		int[] x={1,1,1,1};
		int[] y={1,1,1,1};
		java.awt.Polygon poly= new java.awt.Polygon(x,y,4);
		Hashtable<String, Object> tuple = new Hashtable<String, Object>();
		//tuple.put("ID",6);
		//tuple.put("name","Ziad");
		//tuple.put("isAdult",true);
		//tuple.put("nationality","China");
		//tuple.put("birthdate",new Date(2000,06,02));
		//tuple.put("gpa",1.0);
		tuple.put("shape",poly);
		

		try {
			//d.insertIntoTable("Test1", tuple);
			d.deleteFromTable("Test1", tuple);

		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}

		try {
		  	d.createRTreeIndex("Test1", "shape");
			//d.createBTreeIndex("Test5", "birthdate");
			} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}
		Table obj=Utilities.deserializeTable("Test1");


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
			//TODO: whey u erer? WHAI U EREROR?! pooleez fex des no metud nem .vaeloUf() yes? pulez delt des coad no wrk des metud .velOaf baed
			//TODO:	reamuv metoad
			//TODO: :''''(
			//TODO: ask if it should be thrown as DBApp exception and if we use default format for date
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
					Utilities.binarySearchUpdate(records, 0, records.size() - 1, clusterIdx, clusterKey, strTableName, htblColNameValue);

				}

				Utilities.serializePage(page);
			}

			Utilities.serializeTable(t);

		}
		this.indices = Utilities.loadIndices();
	}


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
			((BPlusTree)	columnTreeIndices.get(key)).delete(value,p,type);
			
		}
		else {
			// Call the Rtree's delete
			((RTree)	columnTreeIndices.get(key)).delete((myPolygon)value,p,type);
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
				String type=metaDataForIndexedColumn[2];
				//check that column is not of type polygon
				if(type.equals("java.awt.Polygon"))
				{
					throw new DBAppException("Cannot create B+ tree index on type polygon");
				}
				//all is well. I can now create the appropriate B+ tree 
				//determining column type to create appropriate BTree
				//System.out.println("Tree should be of type " + type);
				//creating appropriate Btree
				//System.out.println("creating Btree");
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
				String type=metaDataForIndexedColumn[2];
				//check that column is not of type polygon
				if(!type.equals("java.awt.Polygon"))
				{
					throw new DBAppException("Cannot create R tree index on type " + type);
				}
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

