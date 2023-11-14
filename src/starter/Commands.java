package starter;

import BigT.*;
import bufmgr.*;
import diskmgr.pcounter;
import global.*;
import heap.Heapfile;
import iterator.*;

import java.io.*;

import static global.GlobalConst.NUMBUF;

public class Commands {

    static void batchInsert(String dataFile, String tableName, int type) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {
        String dbPath = BigTableMain.getDBPath();
        new SystemDefs(dbPath, BigTableMain.NUM_PAGES, NUMBUF, "Clock");
        pcounter.initialize();

        FileInputStream fileStream;
        BufferedReader br;
        try {
            bigT bigTable = new bigT(tableName);

            fileStream = new FileInputStream(dataFile);
            br = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"));
            String inputStr;

            BOMSkipper.skip(br);
            while ((inputStr = br.readLine()) != null) {
                String[] input = inputStr.split(",");

                //set the map
                Map map = new Map();
                map.setMapHeader(BigTableMain.BIGT_ATTR_TYPES, BigTableMain.BIGT_STR_SIZES);
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[2]));
                map.setValue(input[3]);
                MID mid = bigTable.insertMap(map.getMapByteArray(), type);
            }

            System.out.println("********************************************\n");
            bigTable.printDistributedHeaps();
//            System.out.println("insertions in index file " + bigTable.insertions);
//            System.out.println("deletions in index file " + bigTable.deletions);
            System.out.println("Number of Maps in BigTable: " + bigTable.getMapCnt());
            System.out.println("Number of Distinct Rows:" + bigTable.getRowCnt());
            System.out.println("Number of Distinct Columns:" + bigTable.getColumnCnt());
            System.out.println("Read Count: " + pcounter.rcounter);

            // close bigtable and flush all pages
            bigTable.close();
            fileStream.close();
            br.close();
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();

            System.out.println("Write Count: " + pcounter.wcounter);
            System.out.println("No of buffers: " + NUMBUF);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    static void query(String tableName, Integer orderType, String rowFilter, String colFilter, String valFilter, Integer NUMBUF) throws Exception {
        String dbPath = BigTableMain.getDBPath();
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        pcounter.initialize();
        int matchedResults = 0;

        try {
            bigT bigTable = new bigT(tableName);
            Stream mapStream = bigTable.openStream(orderType, rowFilter, colFilter, valFilter);

            while (true) {
                Map mapObj = mapStream.getNext();
                if (mapObj == null)
                    break;
                mapObj.print();
                matchedResults++;
            }

            System.out.println("\n********************************************\n");
            bigTable.printDistributedHeaps();
            System.out.println("Number of Records matched: " + matchedResults);
            System.out.println("Read count: " + pcounter.rcounter);
            System.out.println("Write count: " + pcounter.wcounter);

            bigTable.close();
            mapStream.closeStream();

        } catch (Exception e) {
            e.printStackTrace();
        }

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();


    }

    static void rowSort(String tableName, String outBigTName, String colName, Integer NUMBUF) throws Exception {
        String dbPath = BigTableMain.getDBPath();
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        pcounter.initialize();


        try {
            bigT bigTable = new bigT(tableName);
            Stream mapStream = bigTable.openStream(6, "*", "*", "*");

            bigT outBigT = new bigT(outBigTName);
            while (true) {
                Map mapObj = mapStream.getNext();
                if (mapObj == null)
                    break;
                mapObj.print();
                outBigT.insertMap(mapObj.getMapByteArray(), 1);
            }

            System.out.println("\n********************************************\n");
            outBigT.printDistributedHeaps();
            System.out.println("Read count: " + pcounter.rcounter);
            System.out.println("Write count: " + pcounter.wcounter);

            outBigT.close();
            bigTable.close();
            mapStream.closeStream();

        } catch (Exception e) {
            e.printStackTrace();
        }
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

    }

    static void getCounts(String tableName, Integer NUMBUF) {
        String dbPath = BigTableMain.getDBPath();
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        pcounter.initialize();

        int mapCount = 0;
        int rowCount = 0;
        int colCount = 0;

        try {
            bigT bigTable = new bigT(tableName);
            mapCount = bigTable.getMapCnt();
            rowCount = bigTable.getRowCnt();
            colCount = bigTable.getColumnCnt();

            System.out.println("\n********************************************\n");
            bigTable.printDistributedHeaps();
            System.out.println("Map count: " + mapCount);
            System.out.println("Row count: " + rowCount);
            System.out.println("Column count: " + colCount);

            bigTable.close();
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void mapInsert(String rowLabel, String colLabel, String value, Integer timestamp, Integer indexType, String tableName, Integer NUMBUF) {
        String dbPath = BigTableMain.getDBPath();
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        pcounter.initialize();

        try {
            bigT bigTable = new bigT(tableName);

            Map map = new Map();
            map.setMapHeader(BigTableMain.BIGT_ATTR_TYPES, BigTableMain.BIGT_STR_SIZES);
            map.setRowLabel(rowLabel);
            map.setColumnLabel(colLabel);
            map.setTimeStamp(timestamp);
            map.setValue(value);
            MID mid = bigTable.insertMap(map.getMapByteArray(), indexType);

            System.out.println("********************************************\n");
            bigTable.printDistributedHeaps();
            System.out.println("Number of Maps in BigTable: " + bigTable.getMapCnt());
            System.out.println("Number of Distinct Rows:" + bigTable.getRowCnt());
            System.out.println("Number of Distinct Columns:" + bigTable.getColumnCnt());
            System.out.println("Read Count: " + pcounter.rcounter);

            // close bigtable and flush all pages
            bigTable.close();
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();

            System.out.println("Write Count: " + pcounter.wcounter);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void createIndex(String tableName, int heapIndex, int indexType) {
        String dbPath = BigTableMain.getDBPath();
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        pcounter.initialize();

        try {
            bigT bigTable = new bigT(tableName);
            bigTable.createAndSyncIndex(heapIndex, indexType);

            System.out.println("********************************************\n");
            System.out.println("Read Count: " + pcounter.rcounter);

            // close bigtable and flush all pages
            bigTable.close();
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();

            System.out.println("Write Count: " + pcounter.wcounter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void rowJoin(String leftTableName, String rightTableName, String outTableName, String columnFilter, String joinType, Integer NUMBUF) throws Exception {
        String dbPath = BigTableMain.getDBPath();
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        pcounter.initialize();
        int matchedResults = 0;

        // TODO: throw error when left or right table does not exist
        try {
            bigT leftBigTable = new bigT(leftTableName);
            bigT outBigTable = new bigT(outTableName);

            BigTableMain.orderType = 1;
            Stream leftStream = leftBigTable.openStream(1, "*", columnFilter, "*");
            int memoryAmount = 10;
            IStream joinStream = rowJoin(memoryAmount, leftStream, rightTableName, columnFilter, joinType);

            while (true) {
                Map map = joinStream.getNext();

                if (map == null) {
                    break;
                }
                map.print();
                outBigTable.insertMap(map.getMapByteArray(), 1);
                matchedResults += 1;
            }

            System.out.println("********************************************\n");
            System.out.println("Number of Records matched: " + matchedResults);
            System.out.println("Read Count: " + pcounter.rcounter);

            leftBigTable.close();
//            leftStream.closeStream();
            joinStream.closeStream();
            outBigTable.close();
            // to prevent page pin exception
            SystemDefs.JavabaseBM.setNumBuffers(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        System.out.println("Write Count: " + pcounter.wcounter);
    }

    static IStream rowJoin(int memAmount, Stream leftStream, java.lang.String rightBigTName, String columnName, java.lang.String joinType) throws Exception {
        short numColumns = (short) 4;
        // Projection reference: Stream.java
        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projection[0] = new FldSpec(rel, 1);
        projection[1] = new FldSpec(rel, 2);
        projection[2] = new FldSpec(rel, 3);
        projection[3] = new FldSpec(rel, 4);

        // Reference: JoinTest.java
        CondExpr[] outputFilter = new CondExpr[3];
        outputFilter[0] = new CondExpr();
        outputFilter[1] = new CondExpr();
        outputFilter[2] = null; // Signals end of output filters. Reference: PredEval.java, Eval(...).
        // Column's are equal
        outputFilter[0].next = null;
        outputFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outputFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outputFilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outputFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
        outputFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        // Values are equal
        outputFilter[1].op = new AttrOperator(AttrOperator.aopEQ);
        outputFilter[1].next = null;
        outputFilter[1].type1 = new AttrType(AttrType.attrSymbol);
        outputFilter[1].type2 = new AttrType(AttrType.attrSymbol);
        outputFilter[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
        outputFilter[1].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 3);

        CondExpr[] rightFilter = null;

        if (joinType.equals("NestedLoop")) {
            String rightHeapFileName = "tempHeapFileJoin" + (System.currentTimeMillis() / 1000);
            Heapfile rightHeapFile = new Heapfile(rightHeapFileName);
            bigT bigTableRight = null; // TODO: CLOSE THIS BIGTABLE
            try {
                bigTableRight = new bigT(rightBigTName);
                Stream bigTableStreamRight = bigTableRight.openStream(1, "*", "*", "*");


                MID mid = new MID();
                Map map = bigTableStreamRight.getNext();
                while (map != null) {
                    rightHeapFile.insertMap(map.getMapByteArray());
                    map = bigTableStreamRight.getNext();
                }
            } catch (Exception e) {
                throw new NestedLoopException(e, "Create new heapfile failed.");
            }
            return new NestedLoopsJoinsMaps(
                BigTableMain.BIGT_ATTR_TYPES,
                numColumns,
                BigTableMain.BIGT_STR_SIZES,
                BigTableMain.BIGT_ATTR_TYPES,
                numColumns,
                BigTableMain.BIGT_STR_SIZES,
                memAmount,
                leftStream,
                rightHeapFileName,
                outputFilter,
                rightFilter,
                projection,
                numColumns
            );
        }
        // sort merge
        else {
            bigT bigTableRight = new bigT(rightBigTName);
            Stream rightStream = bigTableRight.openStream(1, "*", columnName, "*");

            String rightHeapFileName = "tempHeapFileRight" + (System.currentTimeMillis() / 1000);
            Heapfile rightHeapFile = new Heapfile(rightHeapFileName);
            String leftHeapFileName = "tempHeapFileLeft" + (System.currentTimeMillis() / 1000);
            Heapfile leftHeapFile = new Heapfile(leftHeapFileName);

            Map rightMap = rightStream.getNext();
            while (rightMap != null) {
                rightHeapFile.insertMap(rightMap.getMapByteArray());
                rightMap = rightStream.getNext();
            }
            Map leftMap = leftStream.getNext();
            while (leftMap != null) {
                leftHeapFile.insertMap(leftMap.getMapByteArray());
                leftMap = leftStream.getNext();
            }

            int[] sort_fields = new int[1];
            sort_fields[0] = 0;
            int[] sort_fields_lengths = new int[1];
            sort_fields_lengths[0] = BigTableMain.BIGT_STR_SIZES[0];

            FileScan fscanRight = new FileScan(rightHeapFileName, BigTableMain.BIGT_ATTR_TYPES, BigTableMain.BIGT_STR_SIZES,
                    (short) 4, 4, projection, null);
            FileScan fscanLeft = new FileScan(leftHeapFileName, BigTableMain.BIGT_ATTR_TYPES, BigTableMain.BIGT_STR_SIZES,
                    (short) 4, 4, projection, null);

            return new SortMerge(
                BigTableMain.BIGT_ATTR_TYPES,
                numColumns,
                BigTableMain.BIGT_STR_SIZES,
                BigTableMain.BIGT_ATTR_TYPES,
                numColumns,
                BigTableMain.BIGT_STR_SIZES,

                2, // column (1 indexing)
                BigTableMain.BIGT_STR_SIZES[1],
                2,
                BigTableMain.BIGT_STR_SIZES[1],

                10,
                fscanLeft,
                fscanRight,

                false,
                false,
                new TupleOrder(TupleOrder.Ascending),

                outputFilter,
                projection,
                numColumns
            );
        }
    }
}
