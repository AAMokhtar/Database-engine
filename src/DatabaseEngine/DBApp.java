package DatabaseEngine; //change to team name before submitting

import java.util.Hashtable;

public class DBApp {

    public void init(){

    /* this does whatever initialization you would like or leave it empty if there is no code you want to
     execute at application startup */

    }

//Basant's part:

   public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType)
           throws DBAppException{


    }
//Ali's part:

    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{


    }
//Mayar's part:

    public void updateTable(String strTableName, String strKey, Hashtable<String,Object> htblColNameValue)
            throws DBAppException{

    }
//Saeed's part:

    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{

    }



}
