package DatabaseEngine;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;

import java.io.*;

import java.sql.Date;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.*;

import DatabaseEngine.BPlus.BPTExternal;
import DatabaseEngine.BPlus.BPTInternal;
import DatabaseEngine.BPlus.BPTNode;
import DatabaseEngine.BPlus.BPlusTree;
import javafx.util.Pair;

public class Utilities {

	/*
	 * this class has useful operations for metadata and for page storing (Serializing) and loading (deserializing)
	 * This class was fully tested.
	 */
	private static File met = new File("data//metadata.csv");

	//purpose: used to obtain all possible column domains
	public static Vector<String> getPossibleTypes() {
		Vector<String> type = new Vector<String>();
		type.add("java.lang.Integer");
		type.add("java.lang.String");
		type.add("java.lang.Double");
		type.add("java.lang.Boolean");
		type.add("java.util.Date");
		type.add("java.awt.Polygon");
		return type;
	}


	//setup metadata header: to be done in init ONLY.
	public static void initializeMetaData(){

		try {
			File metadata = new File("data//metadata.csv");

			if(metadata.length() ==0) {
				PrintWriter write = new PrintWriter(new FileWriter(met, true));
				write.append("Table Name");
				write.append(",");
				write.append("Column Name");
				write.append(",");
				write.append("Column Type");
				write.append(",");
				write.append("Key");
				write.append(",");
				write.append("Indexed");
				write.append("\n");
				write.flush();
				write.close();
			}
		}

		catch(IOException E) {
			E.printStackTrace();
			System.out.println("problem with writing table info");
		}
	}


	//used everytime a table is created to define its structure.
	public static void writeHeaderIntoMetaData(Hashtable<String, String> colNameType, String tableName, String keyAndIndex) {
		Set<String> colName = colNameType.keySet();

		try {
			//FileWriter write = new FileWriter("metadata.csv");
			PrintWriter write = new PrintWriter(new FileWriter(met, true));
		   for (String n : colName) {
			   write.append(tableName);
			   write.append(",");

			   write.append(n);
			   write.append(",");

			   write.append(colNameType.get(n));
			   write.append(",");

			   if(n==keyAndIndex)
				   write.append("True");

			   else
				   write.append("False");

			   write.append(",");
			   write.append("False");
			   write.append("\n");
		   }

		   write.flush();
		   write.close();
	   }

	   catch(Exception E) {
		   System.out.println("Failed to update metadata.csv!");
		   E.printStackTrace();
	   }

	}
	//initialize properties
	public static void initializeProperties(){
		Properties p=new Properties();
		p.setProperty("MaximumRowsCountinPage","200");
		p.setProperty("NodeSize","15");

		try {
			p.store(new FileWriter("config//DBApp.properties"),"Database engine properties");
		}
		catch (Exception e) {
			System.out.println("Failed to write file!");
		}

	}

	//read properties
	public static Properties readProperties(String path){

		try {
			FileReader reader =new FileReader(path);
			Properties p = new Properties();
			p.load(reader);

			return p;
		}
		catch (Exception e){
			System.out.println("File not found!");
		}

		return null;
	}

	//Used in insert to get meta data for specific table
	//METHOD WORKS. IT HAS BEEN REVIEWED
	public static ArrayList<String[]> readMetaDataForSpecificTable(String strTableName) {

		String tableMetaData = "";

		try {
			String line = "";
			ArrayList<String[]> metaDataForSpecificTable= new ArrayList<String[]>();

		BufferedReader read = new BufferedReader(new FileReader("data//metadata.csv"));
		while ((line = read.readLine()) != null) {
			String[] data = line.split(",");
			if(data[0].equals(strTableName))
			{
				metaDataForSpecificTable.add(data);
			}
		}


			read.close();
			return metaDataForSpecificTable;
		}

		catch(Exception E) {
			System.out.println("Failed to read from metadata.csv!");
			return null;
		}
	}

	//method takes metaData of a specific table as input and output hashtable containing column names and types
	//METHOD WORKS. IT HAS BEEN REVIEWED
	public static Hashtable<String, String> extractNameAndTypeFromMeta(ArrayList<String[]> metaDataForSpecificTable)
	{
		Hashtable<String,String> columnNameColumnType=new Hashtable<String,String>();
		for(String[] column: metaDataForSpecificTable)
		{
			String columnName=column[1];
			String columnType=column[2];
			columnNameColumnType.put(columnName, columnType);
		}
		return columnNameColumnType;
	}

	//method determines the clustering key of a table and it's order in the vector of value
	//METHOD WORKS. IT HAS BEEN REVIEWED
	public static String[] determineValueAndIndexOfClusteringKey(ArrayList<String[]> metaDataForSpecificTable)
	{
		String[] indexAndValue= new String[2];
		for (int i = 0; i < metaDataForSpecificTable.size(); i++) {
			String[] array= metaDataForSpecificTable.get(i);
			if(array[3].equals("True"))
			{
				indexAndValue[0]=array[1];
				indexAndValue[1]=i+"";
			}
		}
		return indexAndValue;
	}

	public static String readMetaData() {

		String tableMetaData = "";

		try {
			String line = "";


			BufferedReader read = new BufferedReader(new FileReader("data//metadata.csv"));
			while ((line = read.readLine()) != null) {
				String[] data = line.split(",");
				tableMetaData+=(Arrays.toString(data)+"\n");
				//WARNING: Arrays.toString() returns data in this form : [...]

		    }


			read.close();
			return tableMetaData;
		}

		catch(Exception E) {
			System.out.println("Failed to read from metadata.csv!");
			return "";
		}
	}

	//readPageCount
	public static int readPageSize(String path) {
		try{
			FileReader reader =new FileReader(path);
			Properties p = new Properties();
			p.load(reader);
			String theNum = p.getProperty("MaximumRowsCountinPage");
			return Integer.parseInt(theNum);}

		catch(IOException E){
			E.printStackTrace();
			System.out.println("Error reading properties");
		}
		return 0;
	}



	public static boolean isTableUnique(String tablename) throws DBAppException {

		try {
			String line = "";
			boolean flag = false;

			BufferedReader read = new BufferedReader(new FileReader("data//metadata.csv"));

			while ((line = read.readLine()) != null) {
				if(!flag) {
					flag = true;
					continue;
				}

				else {
					String[] data = line.split(",");
					if(data[0].equals(tablename)) {
						throw new DBAppException("Table already exists. Please use another name.");

					}

				}

			}

			read.close();
			return true;}

		catch(FileNotFoundException F) {
			F.printStackTrace();
			System.out.println("Metadata file cannot be located.");
			return false;
		}

		catch( IOException I)
		{
			I.printStackTrace();
			System.out.println("Error reading from Metadata file.");
			return false;
		}
	}



	public static void serializePage(Page P) {
		  //store into file (serialize)

				try {
					File file = new File("data//" + "page_" + P.getID() + ".class"); //TODO: fix up path (first item) once directory is set
					FileOutputStream fileAccess;
					fileAccess = new FileOutputStream(file);
					ObjectOutputStream objectAccess = new ObjectOutputStream(fileAccess);
					objectAccess.writeObject(P);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Failed to serialize page.");
				}
	}


	//serialize Table
	public static void serializeTable(Table T) {
		  //store into file (serialize)

				try {
					File file = new File("data//" + "table_" + T.getName() + ".class"); //TODO: fix up path (first item) once directory is set
					FileOutputStream fileAccess;
					fileAccess = new FileOutputStream(file);
					ObjectOutputStream objectAccess = new ObjectOutputStream(fileAccess);
					objectAccess.writeObject(T);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Failed to serialize table.");
				}
	}



	//deserialize page: we pass the id of the page (obtained from the table) and we receive the page object.
	public static Page deserializePage(int pageID) {
		//read from file (deserialize)
		       try {
				FileInputStream readFromFile = new FileInputStream("data//" + "page_" + pageID + ".class");
				ObjectInputStream readObject = new ObjectInputStream(readFromFile);
				Page k = (Page)readObject.readObject();
				readObject.close();
				readFromFile.close();
				return k;

		       }

		       catch(Exception E) {
		    	   System.out.println("Failed to deserialize page. Return value: NULL");
		       }
		       return null;
	}


	public static Table deserializeTable(String tableName) {
		//read from file (deserialize)
		       try {
				FileInputStream readFromFile = new FileInputStream("data//" + "table_" + tableName + ".class");
				ObjectInputStream readObject = new ObjectInputStream(readFromFile);
				Table k = (Table)readObject.readObject();
				readObject.close();
				readFromFile.close();

				return k;

		       }

		       catch(Exception E) {
		    	   System.out.println("Failed to deserialize page. Return value: NULL");
		       }
		       return null;
	}



	//-----------------------------------------------------------------UPDATE HELPERS------------------------------------------------------------------------------------------


	//reads a table name and column name and returns its index
  	public static int returnIndex(String table, String column) throws DBAppException
  	{
  		try {
  			BufferedReader br = new BufferedReader(new FileReader("data//metadata.csv"));
  			String line = br.readLine();
  			if(line==null)
  			{
  				//System.out.println("Metadata is empty.");
  				br.close();
  				//return -1;
  				throw new DBAppException("Metadata is empty.");
  			}

  			line = br.readLine();
  			String[] ar = new String[5]; //metadata row should only contain 5 comma delimited values

  			boolean tableFound = false; //flag if the table is found in metadata
  			boolean colFound = false; //flag if column is found
  			boolean finito = false; //signifies that all table entries were read
  			int index = -1;

  			/*
  			 * if the buffered reader reached the end of file
  			 * or column was found
  			 * or if we reached the end of the consecutive table entries
  			 * stop executing
  			*/
  			while(line != null && !colFound && !finito)
  			{
  				ar = line.split(",");

  				if(ar[0].equals(table))
  				{
  					if(!tableFound) tableFound = true; //flags that the table was found
  					if(ar[1].equals(column)) colFound = true; //flags that the column was found
  					index++;

  				}
  				//if another table name showed up and we already found all the entries of the table, stop executing
  				//this assumes the table entries follow each other in the metadata
  				else if(tableFound) finito = true;

  				line = br.readLine();
  			}

  			br.close();

  			//if table was not found :(
  			if(!tableFound)
  			{
  				//System.out.println("Table name not in metadata");
  				//return -1;
  				throw new DBAppException("Table name not in metadata");
  			}
  			//if column was not found in table's metadata
  			else if(!colFound)
  			{
  				//System.out.println("Column name not found in table metadata entries");
  				//return -1;
  				throw new DBAppException("Column name not found in table metadata entries");
  			}
  			else return index;
  		} catch (IOException e) {
  			e.printStackTrace();
  			return -1;
  		}

  	}

  	//reads a column name and returns an index

  	//read metadata for a specific table to see if the table exists, checks if the columns exist and if the data type matches
  	//TODO: should table entries follow each other in metadata?
  	//if not no need for finito

  	public static boolean updateChecker(String tableName, Hashtable<String,Object> value) throws DBAppException
  	{
  		try {
  			BufferedReader br = new BufferedReader(new FileReader("data//metadata.csv"));
  			String line = br.readLine(); //should contain the first line that contains how the csv file is separated
  			if(line == null)
  			{
  				//System.out.println("Metadata is empty!");
  				br.close();
  				//return false;
  				throw new DBAppException("Metadata is empty!");
  			}

  			line = br.readLine();
  			String[] ar = new String[5]; //metadata row should only contain 5 comma delimited values

  			boolean found = false; //flag if the table is found in metadata
  			boolean finito = false; //flag to see if all table entries are read

  			//hashtable to store column names and their supported types
  			Hashtable<String,String> col = new Hashtable<String,String>();

  			while(line != null && !finito)
  			{
  				ar = line.split(",");

  				if(ar[0].equals(tableName))
  				{
  					if(!found) found = true;
  					col.put(ar[1], ar[2]); //puts column name and its type

  				}
  				//if another table name showed up and we already found all the entries of the table, stop executing
  				//this assumes the table entries follow each other in the metadata
  				else if(found) finito = true;

  				line = br.readLine();
  			}

  			br.close();

  			//if table was not found :(
  			if(!found)
  			{
  				//System.out.println("Table name not in metadata");
  				//return false;
  				throw new DBAppException("Table name not in metadata");
  			}

  			//check if all columns in input hashtable do exist
  			//if they exist check the data type
  			Set<String> colNames = value.keySet();
  			for(String colName: colNames)
  			{
  				//if metadata contains column name of input HT
  				if(col.containsKey(colName))
  				{
  					//if input data is not an instance of the class associated with the column
  					//TODO: remove or just throw error?
  					if(!Class.forName(col.get(colName)).isInstance(value.get(colName)))
  					{
  						//System.out.println("Value inputted for column " + colName +
  								           //" does not correspond with " + col.get(colName));
  						//return false;
  						throw new DBAppException("Value inputted for column " + colName + " does not correspond with " + col.get(colName));
  					}

  				}
  				//if column in input HT does not exist
  				//TODO:remove or just throw error?
  				else
  				{
  					//System.out.println("Column " + colName + " does not exist in table metadata");
  					//return false;
  					throw new DBAppException("Column " + colName + " does not exist in table metadata");
  				}
  			}

  			//else if table exists, columns of input HT exist in the metadata
  			//and the input data matches with the column data type
  			//kda kda meya meya awi
  			return true;

  		} catch (IOException e) 
  		{
  			e.printStackTrace();
  			return false;
  		}
  		catch (ClassNotFoundException e)
  		{
  			e.printStackTrace();
  			return false;
  		}
  	}


  	//returns the column name and type of the clustering key, respectively
  	//note: I don't check if the metadata is empty or table name is nonexistent as these were checked by updateChecker()
  	//and I only call this method if updateChecker() gave an okay
  	public static Pair<String,String> returnClustering(String tableName) throws DBAppException
  	{
  		try {
  			BufferedReader br = new BufferedReader(new FileReader("data//metadata.csv"));
  			br.readLine(); //should read the first line containing how the csv file is ordered
  			String line = br.readLine();
  			String[] ar = new String[5]; //csv file contains only 5 comma delimited values
  			
  			boolean tableFound = false;
  			while(line != null)
  			{
  				ar = line.split(",");
  				if(ar[0].equals(tableName))
  				{
  					tableFound = true;
					if (Boolean.parseBoolean(ar[3]))// found the record with the table name and true for clustering
					{
						return new Pair<String, String>(ar[1], ar[2]); // returns clustering column name and its
																		// respective type
					}
  				}
  				line = br.readLine();
  			}
  			br.close();
  			//if clustering column was not found
  			//System.out.println("Clustering column not found");
  			//return new Pair<String,String>("","");
  			if(!tableFound)
  				throw new DBAppException("Table name " + tableName + " was not found in metadata");
  			
  			throw new DBAppException("Clustering column not found");
  		} catch (IOException e) {
  			e.printStackTrace();
  			return new Pair<String,String>("",""); //return empty column name and type
  		}

  	}


	//binary searches through the vector of records to find the clustering key value
	//if clustering key value is found in the record, it is updated with the values in the HT
	public static void binarySearchUpdate(Vector<Vector> records, int low, int high, int clusterIdx, Comparable clusterKey, String table, Hashtable<String,Object> newVal) throws DBAppException
	{

		if(low<=high)
		{
			int mid = (high-low)/2 + low;
			Comparable clusterValue = (Comparable)records.get(mid).get(clusterIdx);
			if(clusterValue.compareTo(clusterKey)==0)
			{
				//update this record

				//for each key value pair in the HT which contains the values to be updated
				
				Set<String> keys = newVal.keySet();
				
				for(String key : keys)
				{
					int i = returnIndex(table, key);
					records.get(mid).set(i, newVal.get(key)); //ignore the warning, updateChecker already checked the types in the HT matches with metadata
					//TODO: is the TouchDate the last index?
					records.get(mid).set(records.get(mid).size()-1, LocalDateTime.now()); //updates the TouchDate to current time
				}
				/*
				newVal.forEach((key,value) ->
				{
					int i = returnIndex(table, key);
					records.get(mid).set(i, value); //ignore the warning, updateChecker already checked the types in the HT matches with metadata
					//TODO: is the TouchDate the last index?
					records.get(mid).set(records.get(mid).size()-1, LocalDateTime.now()); //updates the TouchDate to current time
				});
				*/

				//check the first and second half if they carry any record with the same clustering key value

				binarySearchUpdate(records, low, mid-1, clusterIdx, clusterKey, table, newVal); //first half
				binarySearchUpdate(records, mid+1, high, clusterIdx, clusterKey, table, newVal); //second half
			}
			else if(clusterValue.compareTo(clusterKey)>0)
				binarySearchUpdate(records, low, mid-1, clusterIdx, clusterKey, table, newVal); //check the first half
			else
				binarySearchUpdate(records, mid+1, high, clusterIdx, clusterKey, table, newVal); //check the second half
		}
		else return;
	}

	//------------------------------========================INDEX HELPERS========================------------------------------------

	//loads all indices from memory and associates columns with their index
	public static Hashtable<String, Hashtable<String, index>> loadIndices() {
		BufferedReader meta = new BufferedReader(new StringReader(readMetaData()));
		Hashtable<String, Hashtable<String, index>> ret = new Hashtable<>();

		try {
			meta.readLine();

			while (meta.ready()) {
				String[] info = meta.readLine().split(", ] \\[");
				//Table Name [0], Column Name [1], Column Type [2], ClusteringKey [3], Indexed [4]

				if (!ret.contains(info[0])) {
					ret.put(info[0], new Hashtable<>());
				}

				if (info[4].charAt(0) == 'T') {
//TODO				ret.get(info[0]).put(info[1], /* tree here */ );
				}

			}
		}
		catch (IOException e){
			return null;
		}



		return ret;
	}

	public static <T extends Comparable<T>> void serializeBPT(BPlusTree<T> tree) { //copy pasted from Basant's (thx XD)

		try {
			File file = new File("data//BPlus//Trees//" + "BPlusTree_" +tree.getName() + ".class");
			FileOutputStream fileAccess;
			fileAccess = new FileOutputStream(file);
			ObjectOutputStream objectAccess = new ObjectOutputStream(fileAccess);
			objectAccess.writeObject(tree);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to serialize tree.");
		}
	}

	public static <T extends Comparable<T>> BPlusTree<T> deserializeBPT(String name) { //copy pasted from Basant's (thx XD)
		//read from file (deserialize)
		try {
			FileInputStream readFromFile = new FileInputStream("data//BPlus//Trees//" + "BPlusTree_" + name+ ".class");
			ObjectInputStream readObject = new ObjectInputStream(readFromFile);
			BPlusTree<T> k = (BPlusTree<T>) readObject.readObject();
			readObject.close();
			readFromFile.close();
			return k;

		}

		catch(Exception E) {
			System.out.println("Failed to deserialize tree. Return value: NULL");
		}
		return null;
	}

	//------------------------========================B+ TREE HELPERS========================---------------------------

	//the best binary search you will ever see. It can even find your lost hopes and dreams in logarithmic time complexity
	public static <T extends Comparable<T>> int selectiveBinarySearch(ArrayList<T> list, T value, String mode){
		int lo = 0;
		int hi = list.size() - 1;
		int mid;
		int index = -1;

		if (mode.equals(">=")) { //smallest index greater than or equal value

			while (lo <= hi) {
				mid = (hi + lo) / 2;

				if (list.get(mid).compareTo(value) >= 0) { //value less or equal
					hi = mid - 1;
					index = mid;
				}
				else { //value greater
					lo = mid + 1;
				}
			}

		}

		if (mode.equals("<=")) { //greatest index smaller than or equal value

			while (lo <= hi) {
				mid = (hi + lo) / 2;

				if (list.get(mid).compareTo(value) > 0) { //value <
					hi = mid - 1;
				}
				else { //value greater or equal
					lo = mid + 1;
					index = mid;
				}
			}
		}

		if (mode.equals("<")) { //greatest index smaller than value

			while (lo <= hi) {
				mid = (hi + lo) / 2;

				if (list.get(mid).compareTo(value) >= 0) { //value less or equal
					hi = mid - 1;
				}
				else { //value greater
					lo = mid + 1;
					index = mid;
				}
			}
		}

		if (mode.equals(">")) { //smallest index greater than value

			while (lo <= hi) {
				mid = (hi + lo) / 2;

				if (list.get(mid).compareTo(value) > 0) { //value <
					hi = mid - 1;
					index = mid;
				}
				else { //value greater or equal
					lo = mid + 1;
				}
			}
		}

		if (mode.equals("=")) { //earliest index equal to value

			while (lo <= hi) {
				mid = (hi + lo) / 2;

				if (list.get(mid).compareTo(value) > 0) { //value <
					hi = mid - 1;
				}
				else if (list.get(mid).compareTo(value) == 0) { //value found
					hi = mid - 1;
					index = mid;
				}
				else { //value greater
					lo = mid + 1;
				}
			}
		}

		return index;
	}

	//recursively searches for a value until it finds and returns the leaf containing it
	// set the "firstNode" flag to true if you want the first leaf in your tree (most left leaf)
	public static <T extends Comparable<T>> BPTExternal<T> findLeaf(BPTNode<T> cur, T value, boolean firstNode){

		if (cur instanceof BPTInternal){
			int key = -1;
			if (!firstNode)
				key = selectiveBinarySearch(cur.getValues(), value, "<="); //find place in array (greatest index less than or equal value)

			return findLeaf(Utilities.deserializeNode(((BPTInternal<T>) cur).getPointers().get(key + 1)),value, firstNode); //down the tree
		}

		return (BPTExternal<T>) cur;
	}

	public static <T extends Comparable<T>> void serializeNode(BPTNode<T> N) { //copy pasted from Basant's (thx XD)

		try {
			File file = new File("data//BPlus//B+_Nodes//" + "Node_" + N.getID() + ".class");
			FileOutputStream fileAccess;
			fileAccess = new FileOutputStream(file);
			ObjectOutputStream objectAccess = new ObjectOutputStream(fileAccess);
			objectAccess.writeObject(N);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to serialize node.");
		}
	}

	public static <T extends Comparable<T>> BPTNode<T> deserializeNode(String nodeID) { //copy pasted from Basant's (thx XD)
		if (nodeID == null) return null;

		try {
			FileInputStream readFromFile = new FileInputStream("data//BPlus//B+_Nodes//" + "Node_" + nodeID + ".class");
			ObjectInputStream readObject = new ObjectInputStream(readFromFile);
			BPTNode<T> k = (BPTNode<T>) readObject.readObject();
			readObject.close();
			readFromFile.close();
			return k;
		}
		catch(Exception E) {
			System.out.println("Failed to deserialize node. Return value: NULL");
		}

		return null;
	}

	//--------------------------======================SELECT HELPERS=====================-------------------------------

	//TODO: explain your code!!!!!! pls!!!!!!!!!!! aboos reglek eshra7
	//fine!!!

	//imprtent pls no dlet tes mesod poleez dees metud gud no dleeet no metld lt me explon metd
	//dees mtus tek 2 et tek 2 oobyekt 1 cowent 1! cles des 2 obct er kles yes? dez cmper 2 veluz
	//wet cndeson met gibs yes no yes? puleeeez no dlet des wrk des guod okee bai
	public static boolean condition(Object a, Object b,Class type , String condition){ //method

		switch (type.getName()){ //switch condition ;)
			case "java.lang.Integer": //some case
				return conditionHelp((Integer) a,(Integer) b,condition); //do something

			case "java.lang.Double": //some other case
				return conditionHelp((Double) a,(Double) b,condition);  //do something else

			case "java.lang.String": //an entirely different case
				return conditionHelp((String) a,(String) b,condition); //do something entirely different

			case "java.util.Date": //i forgot what this is
				return conditionHelp((Date) a,(Date) b,condition); //figure this out yourself

			case "java.lang.Boolean": //final case
				return conditionHelp((Boolean) a,(Boolean) b,condition); //do stuff

			default:break; //some other stuff
		}

		return false; //something
	}

	//takes 2 values and a condition, checks whether the condition is true
	private static <T extends Comparable<T>> boolean conditionHelp(T a, T b, String condition){

		switch (condition){
			case ">":
				return a.compareTo(b) > 0;
			case ">=":
				return a.compareTo(b) >= 0;
			case "<":
				return a.compareTo(b) < 0;
			case "<=":
				return a.compareTo(b) <= 0;
			case "=":
				return a.compareTo(b) == 0;
			default: break;
		}

		return false;
	}

	//searches for a page then the index of a value inside that page. takes a list of pages, a value,
	//and the column number of your value. returns pageID, index respectively
	public static int[] binarySearchValuePage(Vector<Integer> list, Comparable value, int column){
		//smallest element greater than or equal element. returns the pageID and the index respectively

		int lo = 0;
		int hi = list.size() - 1;
		int[] ret ={-1, -1};
		Vector<Vector> elements = null;

		while (lo <= hi){ //binary search for page
			int mid = (lo + hi) / 2;

			Page page = Utilities.deserializePage(list.get(mid)); //get mid page
			elements = page.getPageElements();

			Comparable last = (Comparable) elements.get(elements.size() - 1).get(column); //last element in page

			if (last.compareTo(value) < 0){ // last element is less than value
				lo = mid + 1;
			}
			else { // last element is >= than value
				hi = mid - 1;
				ret[0] = mid;
			}
		}

		lo = 0;
		hi = elements.size() - 1;

		while (lo <= hi && ret[0] != -1){ //binary search in page
			int mid = (lo + hi) / 2;


			if (((Comparable) elements.get(mid).get(column)).compareTo(value) < 0){ // current element is less than value
				lo = mid + 1;
			}
			else { // last element is >= than value
				hi = mid - 1;
				ret[1] = mid;
			}
		}

		return ret;
	}
	
//------------------------------========================MAIN========================------------------------------------
//	public static void main(String[] args) {
//		try {
			//testing returnIndex()
//			System.out.println(returnIndex("kjhrskj","sss"));
			
			//testing updateChecker()
//			Hashtable<String,Object> newVal = new Hashtable<String,Object>();
//			newVal.put("gpa", "ff");
//			newVal.put("name", "bibi");
//			System.out.println(updateChecker("ESTUDIANTE", newVal));
			
			//testing returnClustering()
//			System.out.println(returnClustering("Table Name"));
			
//			gpa,java.lang.Double,False,False
//			ESTUDIANT,ID,java.lang.Integer,False,False
//			ESTUDIANT,isAdult,java.lang.Boolean,False,False
//			ESTUDIANT,name,java.lang.String,True,True
//			ESTUDIANT,deathdate,j
//			Vector<Vector> records = new Vector();
//			Vector<Object> r1 = new Vector<Object>();
//			r1.add(1.2);
//			r1.add(23);
//			r1.add(true);
//			r1.add("bibb");
//			r1.add(new java.util.Date(1999, 3, 7));
//			r1.add(new java.util.Date(2003, 3, 7));
//			records.add(r1);
//			Vector<Object> r3 = new Vector<Object>();
//			r3.add(2.0);
//			r3.add(9);
//			r3.add(true);
//			r3.add("re");
//			r3.add(new java.util.Date(1995, 3, 7));
//			r3.add(new java.util.Date(2003, 3, 7));
//			records.add(r3);
//			Vector<Object> r4 = new Vector<Object>();
//			r4.add(4.3);
//			r4.add(99);
//			r4.add(false);
//			r4.add("re");
//			r4.add(new java.util.Date(1985, 3, 7));
//			r4.add(new java.util.Date(2003, 3, 7));
//			records.add(r4);
//			Vector<Object> r5 = new Vector<Object>();
//			r5.add(9.3);
//			r5.add(18);
//			r5.add(true);
//			r5.add("re");
//			r5.add(new java.util.Date(1975, 3, 7));
//			r5.add(new java.util.Date(2003, 3, 7));
//			records.add(r5);
//			Vector<Object> r2 = new Vector<Object>();
//			r2.add(1.1);
//			r2.add(13);
//			r2.add(false);
//			r2.add("bimb");
//			r2.add(new java.util.Date(2009, 3, 7));
//			r2.add(new java.util.Date(2003, 3, 7));
//			records.add(r2);
//			
//			Hashtable<String, Object> newVal = new Hashtable<String,Object>();
//			newVal.put("gpa",0.7);
//			newVal.put("isAdult",false);
//			System.out.println(records);
//			binarySearchUpdate(records, 0, 4, 3, "re", "ESTUDIANT", newVal);
//			System.out.println(records);
//		} catch (DBAppException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}

}
