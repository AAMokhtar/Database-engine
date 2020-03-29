package DatabaseEngine.RTree;

import java.awt.Polygon;
import java.awt.Rectangle;
import java.io.Serializable;


public class RTreePtr implements Serializable{

	//page part copied from ashraf. thank u :-)
	
	 private int page; //number of page that holds the record(S) of interest.
	 Rectangle boundingBox; //record bounding box

	 // getters/setters
	 public int getPage(){return page;}
	  
	 public void setPage(int page){
		 this.page = page;

	 }
	 
	 public void setBoundingBox(Polygon P) {
		 boundingBox = P.getBounds();
	 }
	 
	 public Rectangle getBoundingBox() {
		 return boundingBox;
	 }
	   
	 public RTreePtr(int page, Polygon P){
	        this.page = page;  
			boundingBox = P.getBounds();
	 }

	  
	 //+ve if the invoker is in a bigger pageNum
	  public int compareTo(Object p) { //compare by page number then index
	       return  this.page - ((RTreePtr)p).page;
      }

	    @Override
	  public boolean equals(Object o) {
	        RTreePtr pointer = (RTreePtr) o;
	        return page == pointer.page && boundingBox.equals(pointer.getBoundingBox()) ;
	  }
}
