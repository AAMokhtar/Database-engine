package DatabaseEngine;

import DatabaseEngine.BPlus.BPTExternal;
import DatabaseEngine.BPlus.BPTInternal;
import DatabaseEngine.BPlus.BPTNode;

import java.io.*;
import java.util.*;
import java.util.Set;

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
	public static void initializeMetaData() {
		try {
        
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
		   write.close();}
		
		catch(Exception E) {
			System.out.println("Failed to initialize metadata.csv!");
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


			   if(n==keyAndIndex)
				   write.append("True");
			
			   else
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
				
		    }
			
		
			read.close();	
			return tableMetaData;
		}
		
		catch(Exception E) {
			System.out.println("Failed to read from metadata.csv!");
			return "";
		}
	}
	
	
	
    	public static boolean isTableUnique(String tablename) {
		
		
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
					return false;
				}
	
				}
				
		    }

			read.close();	
		    return true;
		}
		
		catch(Exception E) {
			System.out.println("Failed to read from metadata.csv!");
			return false;
		}
	}
	
	
	

	//seriaize page
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
				return k;
				
		       }
		       
		       catch(Exception E) {
		    	   System.out.println("Failed to deserialize page. Return value: NULL");
		       }
		       return null;
	}

	//indices

	//loads all indices from memory and associates columns with their index
	public static Hashtable<String, Hashtable<String, index>> loadIndices() throws DBAppException {
		BufferedReader meta = new BufferedReader(new StringReader(readMetaData()));
		Hashtable<String, Hashtable<String, index>> ret = new Hashtable<>();

		try {
			meta.readLine();

			while (meta.ready()){
				String[] info = meta.readLine().split(", ");
				//Table Name [0], Column Name [1], Column Type [2], ClusteringKey [3], Indexed [4]

				if (!ret.contains(info[0])) {
					ret.put(info[0], new Hashtable<>());
				}

				if (info[4].charAt(0) == 'T'){
//TODO				ret.get(info[0]).put(info[1], /* tree here */ );
				}

			}
		}
		catch (Exception e){
			throw new DBAppException("failed to load indices");
		}

		return ret;
	}

	//B+ trees

	//binary search
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
				} else { //value greater
					lo = mid + 1;
				}
			}

		}

		if (mode.equals("<=")) { //greatest index smaller than or equal value

			while (lo <= hi) {
				mid = (hi + lo) / 2;

				if (list.get(mid).compareTo(value) > 0) { //value <
					hi = mid - 1;
				} else { //value greater
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
				} else { //value greater
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
				} else { //value greater
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

	//get leaf inside B+ tree (takes a value, returns the leaf node of that value)
	public static <T extends Comparable<T>> BPTExternal<T> findLeaf(BPTNode<T> cur, T value, boolean firstNode){

		if (cur instanceof BPTInternal){
			int key = selectiveBinarySearch(cur.getValues(), value, "<="); //find place in array
			if (firstNode) key = -1;
			return findLeaf(((BPTInternal<T>) cur).getPointers().get(key + 1),value, firstNode); //down the tree
		}

		return (BPTExternal<T>) cur;
	}

	//select
	public static boolean condition(Object a, Object b,Class type , String condition){

		switch (type.getName()){ //perform query
			case "java.lang.Integer":
				return conditionHelp((Integer) a,(Integer) b,condition);
			case "java.lang.Double":
				return conditionHelp((Double) a,(Double) b,condition);

			case "java.lang.String":
				return conditionHelp((String) a,(String) b,condition);

			case "java.util.Date":
				return conditionHelp((Date) a,(Date) b,condition);

			case "java.lang.Boolean":
				return conditionHelp((Boolean) a,(Boolean) b,condition);

			default:break;
		}
		return false;
	}

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

}