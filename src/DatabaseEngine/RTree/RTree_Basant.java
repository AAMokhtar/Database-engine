package DatabaseEngine.RTree;
import DatabaseEngine.*;

public class RTree_Basant {
	
	/*
	 * R-Tree structure:
	 * 1. a root is needed with these attributes:
	 * 2. 
	 */

	
	/*
	 * helpers needed for deletion: 
	 * (1) findLeaf to get me the position of the leaf. ptr-record or null. Return the result to the big delete
	 * (2) 
	 */
	
	//TODO: place this method in the DBApp. Kept here temporarily for convenience
	public void createRTreeIndex(String strTableName, String strColName) throws DBAppException {
		if(RTreeUtils.tableDoesntExist(strTableName)) {
			//TODO: change DBAppexception constructor to public.
			throw new DBAppException("The table you are trying to create an R Tree index on does not exist.");
		}
		
		else if(RTreeUtils.columnDoesntExist(strTableName,strColName)) {
			throw new DBAppException("The column you are trying to create an R Tree index on does not exist.");
		}
		
		else if(!RTreeUtils.columnTypeFine(strTableName,strColName,"java.awt.Polygon")) {
			throw new DBAppException("The column you are trying to create an R Tree index on is not spatial.");
		}
		
		else if(RTreeUtils.hasIndexAlready(strTableName,strColName)) {
			throw new DBAppException("The column you are tring to create an R Tree index on already has an index.");
		}
		else {
			RTree createdTree = new RTree(strTableName, strColName);
			//TODO: for ashraf: any other action needed?
		}
		
		
	}
}
