package DatabaseEngine.RTree;

import DatabaseEngine.*;
import DatabaseEngine.BPlus.*;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.util.Hashtable;
import java.util.Vector;

public class L_MBR extends MBR{
	
	/*
	 * this is a class for MBRs that are in leaf nodes of an R tree
	 * these carry MBRs of polygons associated with a particular record
	 */
	//TODO: NEW: refer to what exactly??? it will refer to the page that contains our record of interest.
	//NEW: i chose not to use an int only because if we use an object then we can easily latch ashraf's overflowPage class to our code.
	
	private Vector<RTreePtr> entries;
	
	//TODO: NEW FOR ID, PASS ONLY TREEID_UNIQUENUM! It will take care of the MBR!
	//sets the bounds of the MBR rectangle and the min # of MBRs it should carry
	public L_MBR(Rectangle r, int min, String nodeID)
	{
		//nodeType is set 1 as it a leaf
		super(r,1,min,nodeID);
		this.entries = new Vector<RTreePtr>();
		RTreeUtils.serializeNode(this);
	}
	
	//NEW add a record pointer to a leaf. [insertion]
	public void addLeafEntry(RTreePtr p)
	{
		entries.add(p);
		this.setCapacity(this.getCapacity()+1);
	}
	
	//NEW remove a record pointer from a leaf. [deletion]
	public void removeLeafEntry(RTreePtr p) {
		entries.remove(p);
		this.setCapacity(this.getCapacity()-1);
	}
	
	public Vector<RTreePtr> getLeafEntries() {
		return entries;
	}
	
	
}
