package DatabaseEngine.RTree;

import java.awt.Rectangle;
import java.util.Vector;

public class NL_MBR extends MBR{
	
	/*
	 * this is a class for MBRs in non-leaf nodes of an R tree
	 * and thus point to children MBRs it carries within
	 * children MBRs can be leaf MBRs or nonleaf MBRs and thus the children are abstracted to simply MBR
	 * NEW another note: I used add and remove child to give power to the node to control what it points to to easily
	 * restructure both our tree and its file directory. :)
	 */
	
	//NEW: i prefer to check if the MBR is a leaf or intermediary by using instanceof, however i didnt tamper with your code in case
	//u used it in insert. :-)
	
	private Vector<String> childrenNodeIDs; //NEW: the id's of the next MBR's.
	
	//TODO: NEW FOR ID, PASS ONLY TREEID_UNIQUENUM! It will take care of the MBR!
	//sets the bounds of the MBR and the min # of MBRs it should carry
	public NL_MBR(Rectangle r, int min,String nodeID)
	{
		//nodeType is set to 0 as a nonleaf is being created
		super(r,0,min,nodeID); 
		this.childrenNodeIDs = new Vector<String>();
		RTreeUtils.serializeNode(this);
	}
	
	
	public void addChild(String nodeID)
	{
		//TODO: NEW should this be sorted?? nope. It's not a B+ tree, so it doesn't matter.
		this.childrenNodeIDs.add(nodeID);
		this.setCapacity(this.getCapacity()+1);
	}
	
	//NEW: I did this for the delete
	public void removeChild(String nodeID) {
		this.childrenNodeIDs.remove(nodeID);
		this.setCapacity(this.getCapacity()-1);
	}
	
	public Vector<String> getChildren()
	{
		return this.childrenNodeIDs;
	}
	
	
}
