package DatabaseEngine;


import java.awt.Dimension;
import java.awt.Polygon;
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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.*;
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
	public static void initializeMetaData() throws IOException {
        
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
	
	
	//used everytime a table is created to define its structure.
	public static void writeHeaderIntoMetaData(Hashtable<String, String> colNameType, String tableName, String keyAndIndex) throws IOException {
		Set<String> colName = colNameType.keySet();
		
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
	//initialize properties
	public static void initializeProperties() throws IOException {
		Properties p=new Properties();
		p.setProperty("MaximumRowsCountinPage","200");
		p.setProperty("NodeSize","15");

		p.store(new FileWriter("config//DBApp.properties"),"Database engine properties");


	}

	//read properties
	public static Properties readProperties(String path) throws IOException {

			FileReader reader =new FileReader(path);
			Properties p = new Properties();
			p.load(reader);

			return p;
	}

	//Used in insert to get meta data for specific table
	//METHOD WORKS. IT HAS BEEN REVIEWED
	public static ArrayList<String[]> readMetaDataForSpecificTable(String strTableName) throws IOException {

		String tableMetaData = "";

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

	public static String readMetaData() throws IOException {
		
		String tableMetaData = "";
		
			String line = "";
			
			
			BufferedReader read = new BufferedReader(new FileReader("data//metadata.csv"));
			while ((line = read.readLine()) != null) {
				String[] data = line.split(","); 
				tableMetaData+=(Arrays.toString(data)+"\n");
				
		    }
			
		
			read.close();	
			return tableMetaData;
	}
	
	
	
    	public static boolean isTableUnique(String tablename) throws IOException {
		

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
	
	
	
	public static void serializePage(Page P) throws IOException {
		  //store into file (serialize)

					File file = new File("data//" + "page_" + P.getID() + ".class"); //TODO: fix up path (first item) once directory is set
					FileOutputStream fileAccess;
					fileAccess = new FileOutputStream(file);
					ObjectOutputStream objectAccess = new ObjectOutputStream(fileAccess);
					objectAccess.writeObject(P);

	}
	
	
	//serialize Table
	public static void serializeTable(Table T) throws IOException {
		  //store into file (serialize)

					File file = new File("data//" + "table_" + T.getName() + ".class"); //TODO: fix up path (first item) once directory is set
					FileOutputStream fileAccess;
					fileAccess = new FileOutputStream(file);
					ObjectOutputStream objectAccess = new ObjectOutputStream(fileAccess);
					objectAccess.writeObject(T);

	}
	
	
	
	//deserialize page: we pass the id of the page (obtained from the table) and we receive the page object.
	public static Page deserializePage(int pageID) throws IOException, ClassNotFoundException {
		//read from file (deserialize)
				FileInputStream readFromFile = new FileInputStream("data//" + "page_" + pageID + ".class");
				ObjectInputStream readObject = new ObjectInputStream(readFromFile);
				Page k = (Page)readObject.readObject();
				return k;
	}
	
	
	public static Table deserializeTable(String tableName) throws IOException, ClassNotFoundException {
		//read from file (deserialize)
				FileInputStream readFromFile = new FileInputStream("data//" + "table_" + tableName + ".class");
				ObjectInputStream readObject = new ObjectInputStream(readFromFile);
				Table k = (Table)readObject.readObject();
				return k;

	}

	
	
	//-----------------------------------------------------------------UPDATE HELPERS------------------------------------------------------------------------------------------
	
	
	//reads a table name and column name and returns its index
  	public static int returnIndex(String table, String column) throws IOException {
  			BufferedReader br = new BufferedReader(new FileReader("data//metadata.csv"));
  			String line = br.readLine();
  			if(line==null)
  			{
  				System.out.println("Metadata is empty.");
  				br.close();
  				return -1;
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
  				System.out.println("Table name not in metadata");
  				return -1;
  			}
  			//if column was not found in table's metadata
  			else if(!colFound)
  			{
  				System.out.println("Column name not found in table metadata entries");
  				return -1;
  			}
  			else return index;
  	}
  	
  	//reads a column name and returns an index
  	
  	//read metadata for a specific table to see if the table exists, checks if the columns exist and if the data type matches
  	//TODO: should table entries follow each other in metadata?
  	//if not no need for finito
  	
  	public static boolean updateChecker(String tableName, Hashtable<String,Object> value) throws ClassNotFoundException, IOException {
  			BufferedReader br = new BufferedReader(new FileReader("data//metadata.csv"));
  			String line = br.readLine(); //should contain the first line that contains how the csv file is separated
  			if(line == null)
  			{
  				System.out.println("Metadata is empty!");
  				br.close();
  				return false;
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
  				System.out.println("Table name not in metadata");
  				return false;
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
  						System.out.println("Value inputted for column " + colName + 
  								           " does not correspond with " + col.get(colName));
  						return false;
  					}
  					
  				}
  				//if column in input HT does not exist
  				//TODO:remove or just throw error?
  				else
  				{
  					System.out.println("Column " + colName + " does not exist in table metadata");
  					return false;
  				}
  			}
  			
  			//else if table exists, columns of input HT exist in the metadata
  			//and the input data matches with the column data type
  			//kda kda meya meya awi awi
  			return true;
  	}
  	
  	
  	//returns the column name and type of the clustering key, respectively
  	//note: I don't check if the metadata is empty or table name is nonexistent as these were checked by updateChecker()
  	//and I only call this method if updateChecker() gave an okay
  	public static Pair<String,String> returnClustering(String tableName) throws IOException {
  		Pair<String,String> type = new Pair<String,String>("","");
  			BufferedReader br = new BufferedReader(new FileReader("data//metadata.csv"));
  			br.readLine(); //should read the first line containing how the csv file is ordered
  			
  			String line = br.readLine();
  			String[] ar = new String[5]; //csv file contains only 5 comma delimited values
  			
  			while(line != null)
  			{
  				ar = line.split(",");
  				if(ar[0].equals(tableName) && Boolean.parseBoolean(ar[3]));//found the record with the table name and true for clustering
  				{
  					return new Pair<String,String>(ar[1],ar[2]); //returns clustering column name and its respective type
  				}
  				
  			}
  			
  			System.out.println("Clustering column not found");
  			return new Pair<String,String>("","");

  		
  	}
  	
  	//------------------------------------------------------------------POLYGON HELPERS----------------------------------------------------------------------------------------------
  	
  	//polygon toString method
  	//TODO: implemented in the child polygon class
  	public static String toString(Polygon p)
  	{
  		String str = "";
  		for(int i=0;i<p.npoints;i++)
  		{
  			str += "("+p.xpoints[i]+","+p.ypoints[i]+")";
  			
  			if(i<p.npoints-1)
  				str +=",";
  		}
  		
  		return str;
  	}
  	
  	//parses the string and returns a 
  	//TODO: are polygons equal if they have the same size or if they have the same set of points?
  	//implemented the set of points options
  	public static Polygon polygonParse(String s)
  	{
  		String r = s.replace("(", ""); //removes the open parentheses by replacing them with an empty string
  		r = r.replace(")", ""); //removes the close parentheses by replacing them with an empty string
  		
  		String[] points = r.split(","); //will result in a list of strings of int values (alternating between x and y)
  		
  		int n = points.length;
  		if(n%2==0) //if each x has a y
  		{
  			int[] xpoints = new int[n/2]; //int array of x coordinates
  			int[] ypoints = new int[n/2]; //int array of y coordinates
  			
  			int x = 0; //index for the x points array
  			int y = 0; //index for the y points array
  			for(int i=0;i<n;i++)
  			{
  				if(i%2==0) //even indices carry x values 
  				{
  					xpoints[x] = Integer.parseInt(points[i]);
  					x++;
  				}
  				else //odd indices carry y values
  				{
  					ypoints[y] = Integer.parseInt(points[i]);
  					y++;
  				}
  			}
  			
  			n = n/2; //denotes the number of x,y coordinates
  			
  			Polygon p = new Polygon(xpoints, ypoints, n);
  			
  			return p;
  		}
  		else
  		{
  			System.out.println("Number of integer values parsed when parsing for polygon is odd.");
  			return null;
  		}
  	}
  	
  	public static int comparePoly(Polygon p1, Polygon p2)
  	{
  		Dimension d1 = p1.getBounds().getSize();
  		Dimension d2 = p2.getBounds().getSize();
  		
  		int area1 = d1.height*d1.width;
  		int area2 = d2.height*d2.width;
  		
  		return area1-area2;
  		
  	}
  	
  	//produces an array of polygon coordinates in the form [x,y]
  	public static HashSet<Pair<Integer,Integer>> intertwine(Polygon p)
  	{
  		HashSet<Pair<Integer,Integer>> pts = new HashSet<Pair<Integer,Integer>>();
  		Pair<Integer,Integer> pt;
  		int n = p.npoints;
  		for(int i=0;i<n;i++)
  		{
  			pt = new Pair<Integer,Integer>(p.xpoints[i],p.ypoints[i]);
  			pts.add(pt);
  		}
  		return pts;
  	}
  	
  	//compares 2 polygons and sees if they are equal and if so returns true
  	//i.e. they have the exact same points
  	//deals with duplicate points within the same polygon as it uses sets to represent points
  	public static boolean isEqual(Polygon p1, Polygon p2)
  	{
  		HashSet<Pair<Integer,Integer>> pts1 = intertwine(p1); //results in a set of polygon points
  		HashSet<Pair<Integer,Integer>> pts2 = intertwine(p2); 
  		
  		return pts1.equals(pts2);
  		
  	}
  	
  	
  	//----------------------------------------------------------END OF POLYGON HELPERS---------------------------------------------------------------------------------
  	
  	//binary searches through the vector of records to find the clustering key value
  	//if clustering key value is found in the record, it is updated with the values in the HT
  	public static void binarySearchUpdate(Vector<Vector> records, int low, int high, int clusterIdx, Comparable clusterKey, String table, Hashtable<String,Object> newVal)
  	{
  		
  		if(low>=high)
  		{
  			int mid = (high-low)/2 + low;
  			Comparable clusterValue = (Comparable)records.get(mid).get(clusterIdx);
  			
  			if(clusterValue.compareTo(clusterKey)==0)
  			{
  				//update this record
  				
  				//for each key value pair in the HT which contains the values to be updated 
  				newVal.forEach((key,value) ->
  				{
					int i = 0;

					//TODO: I can only catch
					try {
						i = returnIndex(table, key);
					}
					catch (IOException e) {
						e.printStackTrace();
					}
					records.get(mid).set(i, value); //ignore the warning, updateChecker already checked the types in the HT matches with metadata
  					//TODO: is the TouchDate the last index?
  					records.get(mid).set(records.get(mid).size()-1, LocalDateTime.now()); //updates the TouchDate to current time
  				});
  				
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

}
