package DatabaseEngine; //change to team name before submitting

import java.awt.*;
import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class DBAppTest {
    static ArrayList<String> tables;
    static HashMap<String,Hashtable<String,String>> columns;
    static HashMap<String,ArrayList<Hashtable<String,Object>>> tableVals;
    static DBApp db;
    static String[] types = {"java.lang.Integer", "java.lang.String", "java.lang.Boolean"
            , "java.util.Date", "java.lang.Double", "java.awt.Polygon"};

    public static void main(String[] args) throws IOException, DBAppException, ClassNotFoundException {
        tables = new ArrayList<>();
        columns = new HashMap<>();
        tableVals = new HashMap<>();
        db = new DBApp();

        createTable();
        Insert(tables.get(0),10);
    }

    public static String randomString(int length) {
        String res = "";
        for (int i = 0; i < length; i++) {
            res += (char) (Math.random() * ('z' - 'a' + 1) + 'a');
        }
        return res;
    }

    public static void createTable() throws IOException, DBAppException {
        Hashtable<String,String> table = randomTableColumns(7);
        String name = randomString(RandomInt(5, 15));
        tables.add(name);
        columns.put(name,table);
        tableVals.put(name,new ArrayList<>());
        db.createTable(name, pickKey(table), table);
    }

    public static Hashtable<String, String> randomTableColumns(int length) {
        Hashtable<String, String> res = new Hashtable<>();

        for (int i = 0; i < length; i++) {
            res.put(randomString(RandomInt(3, 10)), randomDataType());
        }

        return res;
    }

    public static String randomDataType() {
        return types[RandomInt(0, types.length - 1)];
    }

    public static Double randomDouble(int min, int max){
        Random r = new Random();
       return min + (max - min) * r.nextDouble();
    }

    public static int RandomInt(int min, int max) {
        return (int) (Math.random() * (max - min + 1) + min);
    }

    public static String pickKey(Hashtable<String, String> table) {
        Set s = table.keySet();
        Iterator loop = s.iterator();
        String[] arr = new String[s.size()];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = (String) loop.next();
        }
        String item = arr[RandomInt(0, arr.length - 1)];

        while (table.get(item).equals(types[2])){
            item = arr[RandomInt(0, arr.length - 1)];
        }


        return item;
    }

    public static boolean randomBool(){
        int val = RandomInt(0,1);
        return val == 1;
    }

    public static myPolygon randomPolygon(int length){
        int[] x = new int[length];
        int[] y = new int[length];

        for (int i = 0; i < length; i++) {
            x[i] = RandomInt(-50,50);
            y[i] = RandomInt(0,50);
        }
        return new myPolygon(new Polygon(x,y,length));
    }

    public static Date RandomDate() {
        Date startDate = new GregorianCalendar(1900, Calendar.JANUARY, 0).getTime();
        Date endDate = new GregorianCalendar(2045, Calendar.JANUARY, 0).getTime();
        long random = ThreadLocalRandom.current().nextLong(startDate.getTime(), endDate.getTime());
        return new Date(random);
        }

        public static int randBetween(int start, int end) {
            return start + (int)Math.round(Math.random() * (end - start));
        }


    public static void Insert(String table,int amount) throws DBAppException, IOException, ClassNotFoundException {

        for (int i = 0; i < amount; i++) {
            Hashtable<String, Object> tuple = new Hashtable<>();
            Set keys = columns.get(table).keySet();
            Iterator loop = keys.iterator();
            while (loop.hasNext()){
                String col = (String) loop.next();
                String type = columns.get(table).get(col);

                if (type.equals(types[0])){ //int
                    tuple.put(col, RandomInt(0,100000000));
                }
                if (type.equals(types[1])){ //str
                    tuple.put(col, randomString(RandomInt(0,100)));
                }
                if (type.equals(types[2])){ //bool
                    tuple.put(col, randomBool());
                }
                if (type.equals(types[3])){ //date
                    tuple.put(col, RandomDate());
                }
                if (type.equals(types[4])){ //double
                    tuple.put(col, randomDouble(0,100));
                }
                if (type.equals(types[5])){ //polygon
                    tuple.put(col, randomPolygon(RandomInt(3,15)));
                }
            }

            db.insertIntoTable(table,tuple);
            tableVals.get(table).add(tuple);
        }

    }
}