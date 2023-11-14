package starter;

import bufmgr.*;
import global.AttrType;
import global.SystemDefs;

import java.io.*;


public class BigTableMain {
    public static final AttrType[] BIGT_ATTR_TYPES = new AttrType[]{
            new AttrType(0),
            new AttrType(0),
            new AttrType(1),
            new AttrType(0)
    };

    public static short[] BIGT_STR_SIZES = new short[]{
            (short) 25,  //row
            (short) 25,  //col
            (short) 25  // val
    };
    public static int orderType = 1;
    public static final int NUM_PAGES = 100000;

    // constant for row sort
    public static String rowSortCol;

    public static void main(String[] args) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {

        String input;
        String queryInput;
        String[] queryInputStr;

        while (true) {
            System.out.println("********************************************");
            System.out.println("Press 1 for batchinsert");
            System.out.println("Press 2 for query");
            System.out.println("Press 3 for rowsort");
            System.out.println("Press 4 for getCounts");
            System.out.println("Press 5 for mapinsert");
            System.out.println("Press 6 for createindex");
            System.out.println("Press 7 for rowjoin");
            System.out.println("Press 8 to quit");
            System.out.println("********************************************");
            System.out.print("input: ");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            input = br.readLine();
            if (input.equals(""))
                continue;
            long startTime = System.currentTimeMillis();

            try {
                 // batchinsert command
                 if (input.equalsIgnoreCase("1")) {
                    // batchinsert /Users/kaushal/Downloads/test_data1.csv 1 table1
                    // batchinsert /Users/kaushal/Desktop/phase-1/test_data2.csv 1 testdb9

                    System.out.println("Format: batchinsert DATAFILENAME TYPE BIGTABLENAME");
                    System.out.print("command: ");
                    queryInput = br.readLine();

                    // reset the time
                    startTime = System.currentTimeMillis();

                    queryInputStr = queryInput.trim().split("\\s+");

                    String dataFile = queryInputStr[1];
                    Integer type = Integer.parseInt(queryInputStr[2]);
                    String tableName = queryInputStr[3];

                    Commands.batchInsert(dataFile, tableName, type);
                }
                // Query command
                else if (input.equalsIgnoreCase("2")) {
                    // query table1 1 * * * 500
                    // query table1 1 Greece * * 500

                    System.out.println("Format: query BIGTABLENAME ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");
                    System.out.print("command: ");
                    queryInput = br.readLine();

                    // reset the time
                    startTime = System.currentTimeMillis();

                    queryInputStr = queryInput.trim().split("\\s+");
                    String tableName = queryInputStr[1].trim();

                    orderType = Integer.parseInt(queryInputStr[2]);
                    String rowFilter = queryInputStr[3].trim();
                    String colFilter = queryInputStr[4].trim();
                    String valFilter = queryInputStr[5].trim();
                    Integer NUMBUF = Integer.parseInt(queryInputStr[6]);
                    checkDBMissing();
                    Commands.query(tableName, orderType, rowFilter, colFilter, valFilter, NUMBUF);
                }
                else if (input.equalsIgnoreCase("3")) {
                    // rowsort table1 testsort1 Peafowl 2400
                    System.out.println("Format: rowsort INBTNAME OUTBTNAME COLUMNNAME NUMBUF");
                    System.out.print("command: ");
                    queryInput = br.readLine();

                    // reset the time
                    startTime = System.currentTimeMillis();
                    queryInputStr = queryInput.trim().split("\\s+");
                    String tableName = queryInputStr[1].trim();

                    String outBigTName = queryInputStr[2];
                    String colName = queryInputStr[3];
                    rowSortCol = colName;
                    orderType = 6;
                    Integer NUMBUF = Integer.parseInt(queryInputStr[4]);
                    checkDBMissing();
                    Commands.rowSort(tableName, outBigTName, colName, NUMBUF);
                }
                else if (input.equalsIgnoreCase("4")) {
                    System.out.println("Format: getCounts BTNAME NUMBUF");
                    System.out.print("command: ");
                    queryInput = br.readLine();

                    startTime = System.currentTimeMillis();
                    queryInputStr = queryInput.trim().split("\\s+");
                    String tableName = queryInputStr[1].trim();

                    Integer NUMBUF = Integer.parseInt(queryInputStr[2]);
                    checkDBMissing();
                    Commands.getCounts(tableName, NUMBUF);
                }
                else if (input.equalsIgnoreCase("5")) {
                    System.out.println("Format: mapinsert RL CL VAL TS TYPE BIGTABLENAME NUMBUF");
                    System.out.print("command: ");
                    queryInput = br.readLine();

                    startTime = System.currentTimeMillis();
                    queryInputStr = queryInput.trim().split("\\s+");
                    String rowLabel = queryInputStr[1].trim();
                    String colLabel = queryInputStr[2].trim();
                    String value = queryInputStr[3].trim();
                    Integer timestamp = Integer.parseInt(queryInputStr[4].trim());
                    Integer indexType = Integer.parseInt(queryInputStr[5].trim());
                    String tableName = queryInputStr[6].trim();
                    Integer NUMBUF = Integer.parseInt(queryInputStr[7]);

                    Commands.mapInsert(rowLabel, colLabel, value, timestamp, indexType, tableName, NUMBUF);
                }
                else if (input.equalsIgnoreCase("6")) {
                     System.out.println("Format: createindex BTNAME STORAGE_TYPE NEW_INDEX_TYPE");
                     System.out.print("command: ");
                     queryInput = br.readLine();

                     startTime = System.currentTimeMillis();
                     queryInputStr = queryInput.trim().split("\\s+");
                     String tableName = queryInputStr[1].trim();
                     Integer heapIndex = Integer.parseInt(queryInputStr[2].trim());
                     Integer indexType = Integer.parseInt(queryInputStr[3].trim());

                     Commands.createIndex(tableName, heapIndex, indexType);
                }
                else if (input.equalsIgnoreCase("7")) {
                     System.out.println("Format: rowjoin BTNAME1 BTNAME2 OUTBTNAME COLUMNFILTER JOINTYPE NUMBUF");
                     System.out.print("command: ");
                     queryInput = br.readLine();

                     startTime = System.currentTimeMillis();
                     queryInputStr = queryInput.trim().split("\\s+");
                     String leftTableName = queryInputStr[1].trim();
                     String rightTableName = queryInputStr[2].trim();
                     String outTableName = queryInputStr[3].trim();
                     String columnFilter = queryInputStr[4].trim();
                     String joinType = queryInputStr[5].trim();
                     Integer NUMBUF = Integer.parseInt(queryInputStr[6].trim());

                     Commands.rowJoin(leftTableName, rightTableName, outTableName, columnFilter, joinType, NUMBUF);
                }
                else if(input.equalsIgnoreCase("8")) {
                    break;
                }
                else {
                    System.out.println("Invalid input, enter b/w 1 and 8.\n\n");
                    continue;
                }
            } catch (Exception e) {
                System.out.println("Invalid parameters. Try again.\n\n");
                e.printStackTrace();
                continue;
            }
            SystemDefs.JavabaseBM.flushAllPages();

            final long endTime = System.currentTimeMillis();
            System.out.println("Total time taken: " + (endTime - startTime) / 1000.0 + " seconds");
        }

        System.out.print("quiting...");
    }

    private static boolean checkDBExists() {
        String dbPath = getDBPath();
        File f = new File(dbPath);
//        if (f.exists()) {
//            System.out.println("DB already exists. Exiting.");
//            System.exit(0);
//        }
        return f.exists();
    }

    private static void checkDBMissing() {
        String dbPath = getDBPath();
        File f = new File(dbPath);
        if (!f.exists()) {
            System.out.println("DB does not exist. Exiting.");
            System.exit(0);
        }
//        return f.exists();
    }

    public static String getDBPath() {
        String useId = "user.name";
        String userAccName;
        userAccName = System.getProperty(useId);
        return "/tmp/" + userAccName + ".db";
    }
}
