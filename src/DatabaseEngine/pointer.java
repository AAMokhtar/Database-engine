package DatabaseEngine;

public class pointer implements Comparable<pointer>{
    private int page; //puge num
    private int offset; //index in page

    // getters/setters
    public int getPage(){return page;}
    public int getOffset(){return offset;}
    public void setPage(int page){this.page = page;}
    public void setOffset(int offset){this.offset = offset;}

    public pointer(int page, int offset){
        this.page = page;
        this.offset = offset;
    }

    @Override
    public int compareTo(pointer p) { //compare by page number then index
        if (this.page != p.page)
            return  this.page - p.page;
        return this.offset - p.offset;
    }
}
