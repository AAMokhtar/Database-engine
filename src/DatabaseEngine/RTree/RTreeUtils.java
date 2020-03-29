package DatabaseEngine.RTree;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import DatabaseEngine.DBAppException;
import DatabaseEngine.BPlus.BPTNode;
import DatabaseEngine.BPlus.BPlusTree;

public class RTreeUtils {
	
	//ALL METHODS HERE HAVE BEEN FULLY TESTED.
	
	//PART ONE: R-TREE FILE INTERACTIONS
	
	
	//read the tree node size (n) from the config file.
	public static int readNodeSize(String path) {
		try{
			FileReader reader =new FileReader(path);
			Properties p = new Properties();
			p.load(reader);
			String theNum = p.getProperty("NodeSize");
			return Integer.parseInt(theNum);}

		catch(IOException E){
			E.printStackTrace();
			System.out.println("Error reading R Tree node size.");
		}
		return 0;
	}
	
	
	//serialize R TREE (root + metadata)
	public static void serializeRTree(RTree tree) { 

		try {
			File file = new File("data//RTrees//Trees//" + tree.getRTreeId() + ".class");
			FileOutputStream fileAccess;
			fileAccess = new FileOutputStream(file);
			ObjectOutputStream objectAccess = new ObjectOutputStream(fileAccess);
			objectAccess.writeObject(tree);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to serialize RTree.");
		}
	}
	
	//de-serialize R TREE (root + metadata)
	
	public static RTree deserializeRTree(String treeID) {
		//read from file (deserialize)
		try {
			FileInputStream readFromFile = new FileInputStream("data//RTrees//Trees//" + treeID + ".class");
			ObjectInputStream readObject = new ObjectInputStream(readFromFile);
			RTree k = (RTree) readObject.readObject();
			readObject.close();
			readFromFile.close();
			return k;

		}

		catch(Exception E) {
			System.out.println("Failed to deserialize R tree. Return value: NULL.");
		}
		return null;
	}
	
	
	//serialize MBR 
	public static void serializeNode(MBR Node) { 

		try {
			File file = new File("data//RTrees//R_Nodes//" + Node.getNodeID() + ".class");
			FileOutputStream fileAccess;
			fileAccess = new FileOutputStream(file);
			ObjectOutputStream objectAccess = new ObjectOutputStream(fileAccess);
			objectAccess.writeObject(Node);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to serialize R Tree Node.");
		}
	}
	

   //deserialize MBR 
   //TODO PAY ATTENTION - MBR, NOT L_MBR OR NL_MBR! TYPE CAST
	
	public static MBR deserializeNode(String nodeID) {
		if (nodeID == null) return null;

		try {
			FileInputStream readFromFile = new FileInputStream("data//RTrees//R_Nodes//" + nodeID + ".class");
			ObjectInputStream readObject = new ObjectInputStream(readFromFile);
			MBR k = (MBR) readObject.readObject();
			readObject.close();
			readFromFile.close();
			return k;
		}
		catch(Exception E) {
			System.out.println("Failed to deserialize R Tree Node. Return value: NULL.");
		}

		return null;
	}
	

	//destroys the file of the Node if we decide to delete the node from existence
	
	public static void destroyNodeFile(String nodeID) {
		
		String path = "data//RTrees//R_Nodes//" + nodeID + ".class";
		
		File toBeDeleted = new File(path);
//
//		String path = toBeDeleted.getAbsolutePath();
//		File filePath = new File(path);
		
		if(!(toBeDeleted.delete())) {
			System.out.println("Failed to delete file. Check file directory for debugging purposes.");
		}
		
	}
	
	
	
	
	
	
		
	
	//PART TWO: RECURRENT R-TREE METHOD HELPERS
	
	//check equality IN TERMS OF POINTS
	public static boolean polygonsEqual(Polygon A, Polygon B) {
		if(A.npoints == B.npoints) {
			for (int i = 0; i < A.npoints; i++) {
				if (A.xpoints[i]!=B.xpoints[i]) {
					return false;
				}
			}
			
			for (int i = 0; i < A.npoints; i++) {
				if (A.ypoints[i]!=B.ypoints[i]) {
					return false;
				}
			}
			
			return true;
		}
		
		return false;
	}
	
	//check equality IN TERMS OF AREA
	
	//returns +ve if the first polygon is larger, thanks ALI ^^
 	public int compareAreas(Polygon A, Polygon B)
  	{
  		Dimension d1 = A.getBounds().getSize();
  		Dimension d2 = B.getBounds().getSize();

  		int area1 = d1.height*d1.width;
  		int area2 = d2.height*d2.width;

  		return area1-area2;
  	}
	
	
	
	
	//PART THREE: CHECKERS FOR LEGAL INDEX CREATION
	//next 4 methods used as checks for method createRTreeIndex
	public static boolean tableDoesntExist(String tablename) {

		try {
			String line = "";
			boolean flag = false;

			BufferedReader read = new BufferedReader(new FileReader("data//metadata.csv"));

			while ((line = read.readLine()) != null) {
	
					String[] data = line.split(",");
					if(data[0].equals(tablename)) {
						return false;

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
	
	
	
	public static boolean columnDoesntExist(String tableName, String columnName) {

		try {
			String line = "";
			boolean flag = false;

			BufferedReader read = new BufferedReader(new FileReader("data//metadata.csv"));

			while ((line = read.readLine()) != null) {

					String[] data = line.split(",");
					if(data[0].equals(tableName) &&data[1].equals(columnName)) {
						return false;

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
	
	
	public static boolean columnTypeFine(String tableName, String columnName, String columnType){
		
			try {
				String line = "";

				BufferedReader read = new BufferedReader(new FileReader("data//metadata.csv"));

				while ((line = read.readLine()) != null) {
					
						String[] data = line.split(",");
						if(data[0].equals(tableName) && data[1].equals(columnName) && data[2].equals(columnType)) {
							return true;

						}

				}

				

				read.close();
				return false;}

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
	
	public static boolean hasIndexAlready(String tableName, String columnName) {
		
		try {
			String line = "";

			BufferedReader read = new BufferedReader(new FileReader("data//metadata.csv"));

			while ((line = read.readLine()) != null) {
				
					String[] data = line.split(",");
					if(data[0].equals(tableName) && data[1].equals(columnName) && (data[4].equals("True")||data[4].equals("true"))) {
						return true;

					}

			}

			

			read.close();
			return false;}

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
	
	
	
	//PART FOUR: MAIN METHOD FOR TESTING PURPOSES
	

	public static void main(String[] args) throws IOException {
//		//System.out.println("Test to check node reading.");
 //       System.out.println(readNodeSize("config\\DBApp.properties"));
//		System.out.println(!tableDoesntExist("Test1"));
//		System.out.println(!columnDoesntExist("Test2","area"));
//		System.out.println(columnTypeFine("Test2","nationality","java.awt.Polygon"));
//		System.out.println(hasIndexAlready("Test2", "area"));
//		
//		
//		//tests to check polygon equality
//		int[] xs1 = {1,2,3,4};
//		int[] ys1 = {5,6,7,8,9};
//		int n1 = 3;
//		
//		int[] xs2 = {1,2,3,4};
//		int[] ys2 = {5,6,7};
//		int n2 = 3;
//		
//		int[] xs3 = {1,4,3,4};
//		int[] ys3 = {5,6,7};
//		int n3 = 3;
//		
//		Polygon p1 = new Polygon(xs1,ys1,n1);
//		Polygon p2 = new Polygon(xs2,ys2,n2);
//		Polygon p3 = new Polygon(xs3,ys3,n3);
//		
//		System.out.println(polygonsEqual(p1,p3));
//		
//		Vector<Integer> test = new Vector<Integer>();
//		test.add(1);
//		test.add(2);
//		test.add(3);
//		
//		
//		Iterator it = test.iterator();
//		
//		while(it.hasNext()) {
//			System.out.println(it.next());
//		}
//		
//		RTree rt = new RTree("table1","column1");
//		RTree ret = RTreeUtils.deserializeRTree(rt.getRTreeId());
//		System.out.println(ret.getRTreeId());
//		NL_MBR test = new NL_MBR(new Rectangle(0,2,3,3), rt.getMinNonLeaf(),"table1_column1_num_(0,0)_1_8");
//		serializeNode(test);
//		NL_MBR restored = (NL_MBR)deserializeNode(test.getNodeID());
//		System.out.println(restored.getNodeID());
		//RTreeUtils.destroyNodeFile("table1_column1_num_(0,0)_1_8");
	//	int[] x1 = {1,6,6,1};
//		int[] y1 = {7,7,20,20};
//		
//		int[] x2 = {7,45,45,7};
//		int[] y2 = {7,7,20,20};
//		
//		int[] x3 = {50,60,60,50};
//		int[] y3 = {7,7,20,20};
//		
//		int[] x4 = {0,65,65,0};
//		int[] y4 = {0,0,2,2};
//		
//		Polygon p11 = new Polygon(x1,y1,4);
//		Polygon p22 = new Polygon(x2,y2,4);
//		Polygon p33 = new Polygon(x3,y3,4);
//		Polygon p44 = new Polygon(x4,y4,4);
//		
//		System.out.println(rt.searchPolygonByArea(p44));
		
	    MBR t = new NL_MBR(new Rectangle(0,2,3,3), 7,"table1_column1_4");
	    System.out.println(t.getNodeID());
		RTreeUtils.destroyNodeFile("table1_column1_4_(0.0,2.0)3.03.0");
		
	}
	
	
}
