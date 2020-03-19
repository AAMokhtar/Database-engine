package DatabaseEngine;

import DatabaseEngine.BPlus.BPlusTree;

public class test {
    public static void main(String[] args) throws DBAppException {
        BPlusTree<Integer> tree = new BPlusTree<>("tree",2);
        tree.insert(1,new pointer(1,1),true);
        tree.insert(10,new pointer(1,1),true);
        tree.insert(5,new pointer(1,1),true);
        tree.insert(2,new pointer(1,1),true);
        tree.insert(3,new pointer(1,1),true);
        tree.insert(6,new pointer(1,1),true);
        
    }
}
