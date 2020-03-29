package DatabaseEngine.RTree;

import java.awt.Dimension;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;

import com.sun.javafx.geom.Area;
import com.sun.javafx.geom.Shape;

import DatabaseEngine.myPolygon;

public class RTree implements Serializable{
	private int n;          //max number of MBRs in a node
	private int minNonLeaf; //minimum number of MBRs in a non-leaf node
	private int minLeaf;    //minimum number of MBRS in a leaf node
	private MBR root;       //TODO: ask about this
	private String tableName;
	private String columnName;
	private String RTreeId; //tableName_columnName : always unique since i NEVER allow a second
	//index to be created on the same column! (i trhow dbappexception)
	private int maxNodeId;
	
	//TODO: use the getNodeSize of utils for the n.
	public RTree(String tableName, String columnName)
	{
		//take node size according to configuration file
		this.n = RTreeUtils.readNodeSize("config\\DBApp.properties");
		
		//R tree with a branching factor of 1 or less is not a tree mn asaso
		if(n>=2)
		{
			//tune the parameters
			this.minNonLeaf = (int) (Math.ceil((n+1)/2) - 1);
			this.minLeaf = (int) Math.floor((n+1)/2);
			//no root or anything at first, empty tree	
			root = null;
			maxNodeId = -1; //because no nodes at first
			this.tableName = tableName;
			this.columnName = columnName;
			this.RTreeId = tableName+"_"+columnName;
			
			
			
			/////TESTING
			root = new NL_MBR(new Rectangle(0,0,60,30),1,(RTreeId+"_"+"1"));
			root.setIsRoot(true);
			NL_MBR a1 = new NL_MBR(new Rectangle(0,5,10,20),1,(RTreeId+"_"+"2"));
			NL_MBR a2 = new NL_MBR(new Rectangle(15,6,45,20),1,(RTreeId+"_"+"3"));
			
			L_MBR a11 = new L_MBR(new Rectangle(3,6,7,16),1,(RTreeId+"_"+"4"));
			L_MBR a21 = new L_MBR(new Rectangle(20,7,40,14),1,(RTreeId+"_"+"5"));
			
			int[] x1 = {1,6,6,1};
			int[] y1 = {7,7,20,20};
			
			int[] x2 = {7,45,45,7};
			int[] y2 = {7,7,20,20};
			
			int[] x3 = {50,60,60,50};
			int[] y3 = {7,7,20,20};
			
			Polygon p1 = new Polygon(x1,y1,4);
			Polygon p2 = new Polygon(x2,y2,4);
			Polygon p3 = new Polygon(x3,y3,4);
			
			System.out.println(a11.getLeafEntries().size());
			
			
			
			a11 = (L_MBR) RTreeUtils.deserializeNode(RTreeId+"_"+"4");
			a21 =(L_MBR) RTreeUtils.deserializeNode(RTreeId+"_"+"5");
			a11.addLeafEntry(new RTreePtr(1,p1));
			a21.addLeafEntry(new RTreePtr(2,p2));
			a21.addLeafEntry(new RTreePtr(2,p3));
			RTreeUtils.serializeNode(a11);
			RTreeUtils.serializeNode(a21);
			
			((NL_MBR)root).addChild(RTreeId+"_"+"2");
			((NL_MBR)root).addChild(RTreeId+"_"+"3");
			

			a1 = (NL_MBR) RTreeUtils.deserializeNode(RTreeId+"_"+"2");
			a2 =(NL_MBR) RTreeUtils.deserializeNode(RTreeId+"_"+"3");
			a1.addChild(RTreeId+"_"+"4");
			a2.addChild(RTreeId+"_"+"5");
			RTreeUtils.serializeNode(a1);
			RTreeUtils.serializeNode(a2);
			
			
			
			//serialize the tree.
			RTreeUtils.serializeRTree(this);
		}
		else
			System.out.println("Branching factor cannot be less than 2. Re-tune the config file.");
	}
	
	//getter for n, the max number of MBRs in a node
	public int getN()
	{
		return this.n;
	}
	
	
	//getter for the minimum number of MBRs in a non-leaf node
	public int getMinNonLeaf()
	{
		return this.minNonLeaf;
	}
	
	//getter for the minimum number of MBRs in a leaf node
	public int getMinLeaf()
	{
		return this.minLeaf;
	}
	
	//WARNING: min for ROOT is 1!
	
	public String getTableName() {
		return tableName;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public String getRTreeId() {
		return RTreeId;
	}
	
	public boolean searchPolygonByPoints(Polygon P) {
		return searchPolygonByPoints(P,root);
	}
	
	
	//SEARCH POLYGON HELPERS
	
	public boolean searchPolygonByPoints(Polygon P, MBR curr) {
		//first of all, we need the bounding box of the polygon to obtain 
		Rectangle S = P.getBounds();
		Vector<Boolean> results = new Vector<Boolean>();
		if(curr instanceof NL_MBR) {
			NL_MBR temp = (NL_MBR) curr;
				if(S.intersects(temp)) {
					Vector<MBR> children = temp.getChildren();
					Iterator items = children.iterator();
					while(items.hasNext()) {
						results.add(searchPolygonByPoints(P, (MBR)items.next()));
					}
				}
			return anyIsTrue(results);
		}
		
		else {
			//then we are in a leaf
			L_MBR temp = (L_MBR) curr;
			
			
			//now we need to check for equality of polygons. We do so by checking their points.
			Vector<Polygon> children = temp.getPolygons();
			Iterator items = children.iterator();
			while(items.hasNext()) {
				if(RTreeUtils.polygonsEqual(P, (Polygon)items.next())) {
					return true;
				}
			}
			return false;
		}
	}
	
	
	public boolean anyIsTrue(Vector<Boolean> x) {
		Iterator it = x.iterator();
		while(it.hasNext()) {
			if((Boolean) it.next() == true) {
				return true;}
			}
		
		return false;
	}
	
	
	public boolean searchPolygonByArea(Polygon P) {
		return searchPolygonByArea(P,root);
	}
	
	
	
	public boolean searchPolygonByArea(Polygon P, MBR curr) {
		
		Vector<Boolean> results = new Vector<Boolean>();
		if(curr instanceof NL_MBR) {
			NL_MBR temp = (NL_MBR) curr;
			Vector<MBR> children = temp.getChildren();
			Iterator items = children.iterator();
			while(items.hasNext()) {
				results.add(searchPolygonByArea(P, (MBR)items.next()));
				
			}
			return anyIsTrue(results);
		}
		
		else {
			//then we are in a leaf
			L_MBR temp = (L_MBR) curr;
			
			//now we need to check for equality of polygons. We do so by checking their areas.
			Vector<Polygon> children = temp.getPolygons();
			Iterator items = children.iterator();
			while(items.hasNext()) {
				if(RTreeUtils.compareAreas(P, (Polygon)items.next())==0) {
					return true;
					//for select: add to result set.
				}
			}
			return false;
		}
	}
	


	public void deleteByArea(Polygon P) {
		
	}
	
	public static void main(String[] args) {
		RTree r = new RTree("l","a");
		NL_MBR tmp = (NL_MBR) r.root;
		Vector<MBR> babes = tmp.getChildren();
		
		Iterator it = babes.iterator();
		
		int count = 0;
		
		while(it.hasNext()) {
			System.out.println((((L_MBR)it.next()).getPolygons()).size());
		}
		
//		RTree r = new RTree("ihate","everyone");
//		System.out.println(r.getRTreeId());
//		NL_MBR c= (NL_MBR)r.root;
//		Vector<String> waw = c.getChildren();
//		
//		NL_MBR yuy1 = (NL_MBR) RTreeUtils.deserializeNode("ihate_everyone_2");
//		Vector<String> waw2 = yuy1.getChildren();
//		System.out.println(waw2.size());

		
	
	}
	
	
	
	

	
	
	
}
