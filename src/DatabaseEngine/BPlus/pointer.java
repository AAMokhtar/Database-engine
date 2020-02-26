package DatabaseEngine.BPlus;

public class pointer implements Comparable<pointer>{
    int page;
    int offset;

    pointer(int page, int offset){
        this.page = page;
        this.offset = offset;
    }

    @Override
    public int compareTo(pointer p) {
        if (this.page != p.page)
            return  this.page - p.page;
        return this.offset - p.offset;
    }
}
