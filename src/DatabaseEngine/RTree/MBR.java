package DatabaseEngine.RTree;

import java.awt.Rectangle;
import java.io.Serializable;

abstract class MBR extends Rectangle implements Serializable{
	
	
	//abstract class that parents leaf MBRs and nonleaf MBRs
	private int nodeType; //bit that represents 0 if nonleaf and 1 for leaf
	private int capacity; //number of MBR children the MBR currently carries
	private String nodeID; //NEW to know the id of the node in the following format: treeID_uniqueNum_(x,y)_width_height. 
	private boolean isRoot = false; //NEW for our convenience in special cases - to know if my current node is the root or not.
	//NEW BY DEFAULT, the node IS NOT A ROOT!
	/*
	 * the minimum number of MBRs it can carry
	 * this number differs from leaf to nonleaf
	 * and is appropriately given the correct value when calling the constructor by the RTree
	 * which stores the minimum for each based on n
	 */
	private int min;      
	
	//sets the bounds of the MBR, its type (0=NL,1=L) and the min # of MBRs it should carry
	//TODO: NEW FOR ID, PASS ONLY TREEID_UNIQUENUM! It will take care of the MBR!
	public MBR(Rectangle r, int type, int min, String ID)
	{
		super(r);
		this.nodeType = type;
		this.min = min;
		this.capacity = 0;
		this.nodeID = ID;
				//+"_("+(int)r.getLocation().getX() + "," + (int)r.getLocation().getY()+")_" + r.getWidth() + "_" + r.getHeight();
	}
	
	//the next 3 methods are important to update the node for every deletion, insertion!
	public int getMin()
	{
		return this.min;
	}
	
	public int getCapacity()
	{
		return this.capacity;
	}
	
	public void setCapacity(int n)
	{
		this.capacity = n;
	}
	
	
	//remember nodeType = 1 implies leaf and 0 for nonleaf
	public boolean isLeaf()
	{
		return this.nodeType==1;
	}
	
	
	/*
	 * @param n: this the max number of MBRs a node in the RTree can carry
	 * which is stored in the RTree
	 */
	public boolean isFull(int n)
	{
		return this.capacity == n;
	}
	

	//NEW: the node id, which is treeid_uniqueNumber(unique within tree)
	public void setNodeID(String id) {
		this.nodeID = id;
	}
	
	public String getNodeID() {
		return this.nodeID;
	}
	
	//NEW: setters and getters for the root - used in corner cases of our methods.
	
	public void setIsRoot(boolean b) {
		this.isRoot = b;
	}
	
	public boolean getIsRoot() {
		return this.isRoot;
	}
}
