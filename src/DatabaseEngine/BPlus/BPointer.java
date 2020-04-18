package DatabaseEngine.BPlus;

import DatabaseEngine.Pointer;

import java.io.Serializable;
import java.util.Objects;

public class BPointer implements Comparable<BPointer>, Serializable, Pointer{
    private int page; //puge num
    private int offset; //index in page

    // getters/setters
    public int getPage(){return page;}
    public int getOffset(){return offset;}
    public void setPage(int page){this.page = page;}
    public void setOffset(int offset){this.offset = offset;}

    public BPointer(int page, int offset){
        this.page = page;
        this.offset = offset;
    }

    @Override
    public int compareTo(BPointer p) { //compare by page number then index
        if (this.page != p.page)
            return  this.page - p.page;
        return this.offset - p.offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BPointer pointer = (BPointer) o;
        return page == pointer.page &&
                offset == pointer.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(page, offset);
    }

    @Override
    public String toString() {
        return "(" + page + ", " + offset+")";
    }
}
