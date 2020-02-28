package DatabaseEngine; //change to team name before submitting

import DatabaseEngine.BPlus.BPlusTree;

import javax.swing.plaf.synth.ColorType;
import java.util.*;

public class DBApp {
	private Hashtable<String, Hashtable<String, index>> indices; // table name -> column name -> tree (M2 code)

	public void init() throws DBAppException {
		Utilities.initializeMetaData();
		Utilities.initializeProperties();
//		indices = Utilities.loadIndices(); (M2 code)

		//TODO: add any other "initializing code" here!
	}
	
	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType ) throws DBAppException {
		
		Table t = new Table(strTableName,strClusteringKeyColumn,htblColNameType);
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
////       d.init();
//		try {
//			d.createTable("Table Name", "name", t);
//		} catch (DBAppException e) {
//			System.out.println(e.getMessage());
//			e.printStackTrace();	}
//	}
    
		
		//TODO: FOR ALL Y'ALL: PLEASE KEEP TESTS IN COMMENTS!! 

    
//Ali's part:

    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{


    }
//Mayar's part:

    public void updateTable(String strTableName, String strKey, Hashtable<String,Object> htblColNameValue)
            throws DBAppException{

    }
//Saeed's part:

    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{
   		Table t = Utilities.deserializeTable(strTableName);
    	t.delete(htblColNameValue);
    	
    }

//----------------------------------M2------------------------------------------
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException{
		Set<pointer> resultPointers = null;

		//----=not enough operators=-----
		if (strarrOperators.length != arrSQLTerms.length - 1)
			throw new DBAppException("Operator missing!");

		int i = 0; //operator index
		for(SQLTerm cur: arrSQLTerms){ //for each SQLTerm

		//------------------------------------Integrity checks------------------------------------

			//--current term is complete-
			if (cur._strTableName == null || cur._strColumnName == null
					|| cur._strOperator == null || cur._objValue == null){
				throw new DBAppException("Incomplete SQLTerm!");
			}

			//--------table exists-------
			Table cur_table = Utilities.deserializeTable(cur._strTableName);
			if (cur_table == null) throw new DBAppException("Table not found!");

			//---extract table metadata--
			ArrayList<String[]> metaData = Utilities.readMetaDataForSpecificTable(cur._strTableName);
			if (metaData == null) throw new DBAppException("Cannot fetch metadata!");

			//-------column exists-------
			String[] colInfo = null;

			for(String[] col : metaData){
				if (col[1].equals(cur._strColumnName))
					colInfo = col;
			}

			if (colInfo == null) throw new DBAppException("Attribute not found!");

			//-------correct type--------
			Class colType = null;

			try {
				colType = Class.forName(colInfo[2]);
			}
			catch (Exception e){
				throw new DBAppException("Column type does not exist!");
			}
			if (!colType.isInstance(cur._objValue)){
				throw new DBAppException("term value is incompatible with column type!");
			}

			//------correct operator-----
			switch (cur._strOperator){
				case ">":
				case ">=":
				case "<":
				case "<=":
				case "=":break;
				default: throw new DBAppException("Unrecognized operator!");
			}

			//------indexed column?------
			Boolean Indexed = colInfo[4].charAt(0) == 'T';

			//-------retrieve Index------
			index tree = null;
			if (indices.get(cur._strTableName).contains(cur._strColumnName))
				tree = indices.get(cur._strTableName).get(cur._strColumnName);

			if (Indexed && tree == null)
				throw new DBAppException("Could not find index!");


		//------------------------------------Execution------------------------------------
			Set<pointer> queryResult = null;

			if (Indexed){ //binary search in tree

				Class polygon = null;
				try { polygon = Class.forName("java.awt.Polygon"); } catch (Exception e){}; //get polygpn class
				if (polygon == null) throw new DBAppException("a problem occurred!"); //somehow polygon class is not found

				if (polygon.isAssignableFrom(colType)){ //use R tree
				//TODO: R tree query
				}

				else { //use B+ trees

					switch (colType.getName()){ //perform query
						case "java.lang.Integer":
							queryResult = ((BPlusTree) tree).search((Integer) cur._objValue, cur._strOperator);
							break;

						case "java.lang.Double":
							queryResult = ((BPlusTree) tree).search((Double) cur._objValue, cur._strOperator);
							break;

						case "java.lang.String":
							queryResult = ((BPlusTree) tree).search((String) cur._objValue, cur._strOperator);
							break;

						case "java.util.Date":
							queryResult = ((BPlusTree) tree).search((Date) cur._objValue, cur._strOperator);
							break;

						case "java.lang.Boolean":
							queryResult = ((BPlusTree) tree).search((Boolean) cur._objValue, cur._strOperator);
							break;

						default:break;
					}
				}
			}

			else { //linear search in records
				for(int pageId : cur_table.getPages()){
					Vector<Vector> page = Utilities.deserializePage(pageId).getPageElements();
					for(Vector tuple : page){

					}
				}
			}

			if (queryResult == null) throw new DBAppException("Query failed!"); //could not perform query
			//-----------perform set operation-----------

			if (resultPointers == null) resultPointers = queryResult; //first query

			else { // not first
				switch (strarrOperators[i++].toUpperCase()){
					case "AND": resultPointers = resultPointers.AND(queryResult);break;
					case "OR": resultPointers = resultPointers.OR(queryResult);break;
					case "XOR": resultPointers = resultPointers.XOR(queryResult);break;
					default: throw new DBAppException("invalid set operation!");
				}
			}
		}
		
		//TODO get records from pointers
		return null;
	}

	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException{
		//TODO
	}

	public void createRTreeIndex(String strTableName, String strColName) throws DBAppException{
		//TODO
	}
}
