package DatabaseEngine;

import java.awt.Polygon;

public class myPolygon implements Comparable<myPolygon>{
    Polygon shape;
    int area;

    public myPolygon(Polygon s) {
        // TODO Auto-generated constructor stub
        shape=s;
        java.awt.Rectangle boundingBox=shape.getBounds();
        area=boundingBox.height*boundingBox.width;
    }

    @Override
    public int compareTo(myPolygon poly) {
        // TODO Auto-generated method stub
        return this.area-poly.area;
    }
    public String toString()
    {
        return shape.toString();
    }
}
