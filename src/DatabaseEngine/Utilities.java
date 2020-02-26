package DatabaseEngine;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

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
	
	
	
	//TODO: FOR ALI: edit this code such that you get back data for a specific table 
	//TODO: this, when executed, prints metadata content. 
	//TODO: you can copy this method and edit it to read only specific parts of the metadata file
	//TODO: BUT KEEP THIS METHOD INTACT THX
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

	//Btrees

	//binary search
	public static <T extends Comparable<T>> int  binarySearchLeastGreaterEq(ArrayList<T> list, T value,String mode){
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

		return index;
	}

}
