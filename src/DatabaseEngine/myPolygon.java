package DatabaseEngine;

import java.awt.Dimension;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.util.HashSet;

import javafx.util.Pair;

public class myPolygon extends Polygon implements Comparable<myPolygon>{
    Polygon shape;
    int area;

    public myPolygon(Polygon s) {
        // TODO Auto-generated constructor stub
        shape=s;
        this.npoints = shape.npoints;
        this.xpoints = shape.xpoints;
        this.ypoints = shape.ypoints;
        this.bounds = shape.getBounds();
        this.area = this.bounds.height*this.bounds.width;
        java.awt.Rectangle boundingBox=shape.getBounds();
        area=boundingBox.height*boundingBox.width;
    }

//    @Override
//    public int compareTo(myPolygon poly) {
//        // TODO Auto-generated method stub
//        return this.area-poly.area;
//    }
    
    //polygon toString method
  	//TODO: implemented in the child polygon class
  	public String toString()
  	{
  		String str = "";
  		for(int i=0;i<this.shape.npoints;i++)
  		{
  			str += "("+this.shape.xpoints[i]+","+this.shape.ypoints[i]+")";

  			if(i<this.shape.npoints-1)
  				str +=",";
  		}
  		return str;
  	}
  

  	//parses the string and returns a
  	//TODO: are polygons equal if they have the same size or if they have the same set of points?
  	//implemented the set of points options
  	public static myPolygon parsePolygon(String s) throws DBAppException
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
  			myPolygon m = new myPolygon(p);
  			return m;
  		}
  		else
  		{
  			//System.out.println("Number of integer values parsed when parsing for polygon is odd.");
  			//return null;
  			throw new DBAppException("Number of integer values parsed when parsing for polygon is odd.");
  		}
  	}

  	public int compareTo(myPolygon p)
  	{
  		Dimension d1 = this.shape.getBounds().getSize();
  		Dimension d2 = p.shape.getBounds().getSize();

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

	public boolean equals(myPolygon p2){
		return myPolygon.isEqual(this.shape,p2.shape);

	}
  	
//  	public static void main(String[] args) {
//		int[] x = {1,3,1,3};
//		int[] y = {1,3,3,1};
//		myPolygon p = new myPolygon(new Polygon(x,y,4));
//		int[] x1 = {1,2,1,2};
//		int[] y1 = {1,2,2,1};
//		myPolygon p1 = new myPolygon(new Polygon(x1,y1,4));
//		System.out.println(p);
//		System.out.println(p1.compareTo(p));
//		
//	}
}
