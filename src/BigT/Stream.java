package BigT;

import btree.*;
import starter.BigTableMain;
import diskmgr.OutOfSpaceException;
import global.MID;
import global.RID;
import global.TupleOrder;
import global.GlobalConst;
import heap.Heapfile;
import heap.MapScan;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.MapSort;
import iterator.RelSpec;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * Stream Class to initialize a stream of maps on bigTable.
 */
public class Stream implements IStream {
    private final String rowFilter;
    private final String columnFilter;
    private final String valueFilter;
    private bigT bigtable;
    boolean[] scanAllArr;
    private String filterAll;
    // regex to split the low and high range
    private String lastChar;
    private BTFileScan[] btreeScanners;
    public Heapfile tempHeapFile;
    public String tempHeapFileName;
    private MapSort sortObj;
    private MapScan mapScan;
    private int orderType;
    private int testIndexMatchedCount;

    /**
     *  orderType is for ordering the results by
     *    1, then results are first ordered in row label, then column label, then time stamp
     *    2, then results are first ordered in column label, then row label, then time stamp
     *    3, then results are first ordered in row label, then time stamp
     *    4, then results are first ordered in column label, then time stamp
     *    6, then results are ordered in time stamp
     *
     * @param bigTable Object containing all the maps in bigt table.
     * @param orderType Order type for sorting/ ordering of output.
     * @param rowFilter Filtering based on row values.
     * @param columnFilter Filtering based on column values.
     * @param valueFilter Filtering based on value field.
     * @throws Exception Throws generic Exception.
     */
    public Stream(bigT bigTable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        this.bigtable = bigTable;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        this.orderType = orderType;
        this.filterAll = GlobalConst.filterAllQuery;
        this.lastChar = GlobalConst.lastChar;
        this.btreeScanners = new BTFileScan[5];
        this.scanAllArr = new boolean[5];
        this.testIndexMatchedCount = 0;

        Random random = new Random();
        this.tempHeapFileName = "tempSort1" + random.nextInt(1000);
        this.tempHeapFile = new Heapfile(tempHeapFileName);

        for (int i=0; i<5; i++) {
            constructQueryKeys(i);
        }

//        System.out.println("ScanAllArr " + Arrays.toString(this.scanAllArr));
        for (int i=0; i<5; i++) {
            aggregateData(i);
        }

//        System.out.println("Matched from index file " + this.testIndexMatchedCount);

        // Perform sorting
        sortResultsByOrderType();

    }

    /**
     * Find start and end string for querying from the database
     * @throws Exception Throws generic exception.
     */
    private void constructQueryKeys(int heapIndex) throws IteratorException, ConstructPageException, KeyNotMatchException, PinPageException, IOException, UnpinPageException {
        StringKey lo_key = null;
        StringKey hi_key = null;
        int indexType = heapIndex + 1;

        // check the indexing type of the database
        switch (indexType) {
            case 1:
                this.scanAllArr[heapIndex] = true;
                break;
            case 2:
                this.scanAllArr[heapIndex] = false;
                StringKey[] output1 = type_1_filter(heapIndex);
                lo_key = output1[0];
                hi_key = output1[1];
                break;
            case 3:
                this.scanAllArr[heapIndex] = false;
                StringKey[] output2 = type_2_filter(heapIndex);
                lo_key = output2[0];
                hi_key = output2[1];
                break;
            case 4:
                this.scanAllArr[heapIndex] = false;
                StringKey[] output3 = type_3_filter(heapIndex);
                lo_key = output3[0];
                hi_key = output3[1];
                break;
            case 5:
                this.scanAllArr[heapIndex] = false;
                StringKey[] output4 = type_4_filter(heapIndex);
                lo_key = output4[0];
                hi_key = output4[1];
                break;
        }

        if (!this.scanAllArr[heapIndex]) {
            BTreeFile indexFile = this.bigtable.indexFiles[heapIndex][heapIndex];
            this.btreeScanners[heapIndex] = indexFile.new_scan(lo_key, hi_key);
        }
    }

    private StringKey[] type_1_filter(int heapIndex) {
        StringKey lo_key = null, hi_key = null;
        // if row filter is "*"
        if (rowFilter.equals(filterAll)) {
            this.scanAllArr[heapIndex] = true;
        } else {
            // if row filter is a range query
            if (rowFilter.matches(GlobalConst.rangeRegex)) {
                String[] range = rowFilter.replaceAll(GlobalConst.squareBracketRegex, "").split(",");
                lo_key = new StringKey(range[0]);
                hi_key = new StringKey(range[1] + this.lastChar);
            } else {
                lo_key = new StringKey(rowFilter);
                hi_key = new StringKey(rowFilter + this.lastChar);
            }
        }
        return new StringKey[]{lo_key, hi_key};
    }

    private StringKey[] type_2_filter(int heapIndex) {
        StringKey lo_key = null, hi_key = null;
        if (columnFilter.equals(filterAll)) {
            this.scanAllArr[heapIndex] = true;
        } else {
            if (columnFilter.matches(GlobalConst.rangeRegex)) {
                String[] range = columnFilter.replaceAll(GlobalConst.squareBracketRegex, "").split(",");
                lo_key = new StringKey(range[0]);
                hi_key = new StringKey(range[1] + this.lastChar);
            } else {
                lo_key = new StringKey(columnFilter);
                hi_key = new StringKey(columnFilter + this.lastChar);
            }
        }
        return new StringKey[]{lo_key, hi_key};
    }

    private StringKey[] type_3_filter(int heapIndex) {
        StringKey lo_key = null, hi_key = null;
        if ((rowFilter.equals(filterAll)) && (columnFilter.equals(filterAll))) {
            this.scanAllArr[heapIndex] = true;
        } else {
            if ((rowFilter.matches(GlobalConst.rangeRegex)) && (columnFilter.matches(GlobalConst.rangeRegex))) {

                String[] rowRange = rowFilter.replaceAll(GlobalConst.squareBracketRegex, "").split(",");
                String[] columnRange = columnFilter.replaceAll(GlobalConst.squareBracketRegex, "").split(",");
                lo_key = new StringKey(columnRange[0] + GlobalConst.DELIMITER + rowRange[0]);
                hi_key = new StringKey(columnRange[1] + GlobalConst.DELIMITER + rowRange[1] + this.lastChar);

                //check row range and column fixed/*
            } else if ((rowFilter.matches(GlobalConst.rangeRegex)) && (!columnFilter.matches(GlobalConst.rangeRegex))) {
                String[] rowRange = rowFilter.replaceAll(GlobalConst.squareBracketRegex, "").split(",");
                if (columnFilter.equals(filterAll)) {
                    this.scanAllArr[heapIndex] = true;
                } else {
                    lo_key = new StringKey(columnFilter + GlobalConst.DELIMITER + rowRange[0]);
                    hi_key = new StringKey(columnFilter + GlobalConst.DELIMITER + rowRange[1] + this.lastChar);
                }
                // check column range and row fixed/*
            } else if ((!rowFilter.matches(GlobalConst.rangeRegex)) && (columnFilter.matches(GlobalConst.rangeRegex))) {
                String[] columnRange = columnFilter.replaceAll(GlobalConst.squareBracketRegex, "").split(",");
                if (rowFilter.equals(filterAll)) {
                    lo_key = new StringKey(columnRange[0]);
                    hi_key = new StringKey(columnRange[1] + this.lastChar);
                } else {
                    lo_key = new StringKey(columnRange[0] + GlobalConst.DELIMITER + rowFilter);
                    hi_key = new StringKey(columnRange[1] + GlobalConst.DELIMITER + rowFilter + this.lastChar);
                }

                //row and col are fixed val or *,fixed fixed,*
            } else {
                if (columnFilter.equals(filterAll)) {
                    this.scanAllArr[heapIndex] = true;
                } else if (rowFilter.equals(filterAll)) {
                    lo_key = hi_key = new StringKey(columnFilter);
                } else {
                    lo_key = new StringKey(columnFilter + GlobalConst.DELIMITER + rowFilter);
                    hi_key = new StringKey(columnFilter + GlobalConst.DELIMITER + rowFilter + this.lastChar);
                }
            }
        }
        return new StringKey[]{lo_key, hi_key};
    }

    private StringKey[] type_4_filter(int heapIndex) {
        StringKey lo_key = null, hi_key = null;
        if ((valueFilter.equals(filterAll)) && (rowFilter.equals(filterAll))) {
            this.scanAllArr[heapIndex] = true;
        } else {

            // check if both range
            if ((valueFilter.matches(GlobalConst.rangeRegex)) && (rowFilter.matches(GlobalConst.rangeRegex))) {

                String[] valueRange = valueFilter.replaceAll(GlobalConst.squareBracketRegex, "").split(",");
                String[] rowRange = rowFilter.replaceAll(GlobalConst.squareBracketRegex, "").split(",");
                lo_key = new StringKey(rowRange[0] + GlobalConst.DELIMITER + valueRange[0]);
                hi_key = new StringKey(rowRange[1] + GlobalConst.DELIMITER + valueRange[1] + this.lastChar);

                //check row range and column fixed/*
            } else if ((valueFilter.matches(GlobalConst.rangeRegex)) && (!rowFilter.matches(GlobalConst.rangeRegex))) {
                String[] valueRange = valueFilter.replaceAll(GlobalConst.squareBracketRegex, "").split(",");
                if (rowFilter.equals(filterAll)) {
                    this.scanAllArr[heapIndex] = true;
                } else {
                    lo_key = new StringKey(rowFilter + GlobalConst.DELIMITER + valueRange[0]);
                    hi_key = new StringKey(rowFilter + GlobalConst.DELIMITER + valueRange[1] + this.lastChar);
                }
                // check column range and row fixed/*
            } else if ((!valueFilter.matches(GlobalConst.rangeRegex)) && (rowFilter.matches(GlobalConst.rangeRegex))) {
                String[] rowRange = rowFilter.replaceAll(GlobalConst.squareBracketRegex, "").split(",");
                if (valueFilter.equals(filterAll)) {
                    lo_key = new StringKey(rowRange[0]);
                    hi_key = new StringKey(rowRange[1] + this.lastChar);
                } else {

                    lo_key = new StringKey(rowRange[0] + GlobalConst.DELIMITER + valueFilter);
                    hi_key = new StringKey(rowRange[1] + GlobalConst.DELIMITER + valueFilter + this.lastChar);
                }

                //row and col are fixed val or *,fixed fixed,*
            } else {
                if (rowFilter.equals(filterAll)) {
                    // *, fixed
                    this.scanAllArr[heapIndex] = true;
                } else if (valueFilter.equals(filterAll)) {
                    // fixed, *
                    lo_key = new StringKey(rowFilter);
                    hi_key = new StringKey(rowFilter + lastChar);
                } else {
                    // both fixed
                    lo_key = new StringKey(rowFilter + GlobalConst.DELIMITER + valueFilter);
                    hi_key = new StringKey(rowFilter + GlobalConst.DELIMITER + valueFilter + this.lastChar);
                }
            }
        }
        return new StringKey[]{lo_key, hi_key};
    }


    private void aggregateData(int heapIndex) throws Exception {
        int matchedRecords = 0;

        //scanning whole bigt file.
        if (this.scanAllArr[heapIndex]) {
            MID midObj = new MID();
            if (bigtable.heapfiles[heapIndex] != null) {
                mapScan = bigtable.heapfiles[heapIndex].openMapScan();
                Map map = this.mapScan.getNext(midObj);
                while (map != null) {
                    if (isMatched(map, "row", rowFilter)
                     && isMatched(map, "column", columnFilter)
                     && isMatched(map, "value", valueFilter)) {
                        tempHeapFile.insertMap(map.getMapByteArray());
                        matchedRecords++;
                    }
                    map = mapScan.getNext(midObj);
                }
            }
        }


        else {

            KeyDataEntry entry = this.btreeScanners[heapIndex].get_next();

            while (entry != null) {
//                System.out.println("Index " + indexType + " , Heapfile " + heapIndex + " total count " + bigtable.heapfiles[heapIndex].getRecCnt());
                RID rid = ((LeafData) entry.data).getData();
                if (rid != null) {
                    MID midFromRid = new MID(rid.pageNo, rid.slotNo);

                    if (bigtable.heapfiles[heapIndex] != null) {
                        Heapfile currHeapFile = bigtable.heapfiles[heapIndex];

                        Map map = currHeapFile.getMap(midFromRid);
                        if (map != null
                                && isMatched(map, "row", rowFilter)
                                && isMatched(map, "column", columnFilter)
                                && isMatched(map, "value", valueFilter)) {
                            tempHeapFile.insertMap(map.getMapByteArray());
                            matchedRecords++;
                        }
                    }
                }
                entry = this.btreeScanners[heapIndex].get_next();
            }
        }
//        System.out.println("matched records" + matchedRecords);

    }

    private void sortResultsByOrderType() {
        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projection[0] = new FldSpec(rel, 1);
        projection[1] = new FldSpec(rel, 2);
        projection[2] = new FldSpec(rel, 3);
        projection[3] = new FldSpec(rel, 4);

        FileScan fscan = null;

        try {
            fscan = new FileScan(this.tempHeapFileName, BigTableMain.BIGT_ATTR_TYPES, BigTableMain.BIGT_STR_SIZES, (short) 4, 4, projection, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        int sortField;
        int sortFieldLength;
        int num_pages = 10;

        switch (orderType) {
            case 1:
            case 3:
            case 6:
                sortField = 1;
                sortFieldLength = BigTableMain.BIGT_STR_SIZES[0];
                break;
            case 2:
            case 4:
                sortField = 2;
                sortFieldLength = BigTableMain.BIGT_STR_SIZES[1];
                break;
            case 5:
                sortField = 3;
                sortFieldLength = BigTableMain.BIGT_STR_SIZES[2];
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + orderType);
        }
        try {
            this.sortObj = new MapSort(BigTableMain.BIGT_ATTR_TYPES, BigTableMain.BIGT_STR_SIZES, fscan, sortField, new TupleOrder(TupleOrder.Ascending), num_pages, sortFieldLength);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Returns true if map value is matched with the filter
     * @param map Map object to compare field with filter.
     * @param field field type to be compared - row, column or value.
     * @param filter Filter on fields - row, column or value.
     * @return
     * @throws Exception
     */
    private boolean isMatched(Map map, String field, String filter) throws Exception {
        // check if the map data lies between the low and high filters
        if (filter.matches(GlobalConst.rangeRegex)) {
            String[] range = filter.replaceAll(GlobalConst.squareBracketRegex, "").split(",");
            return map.getGenericValue(field).compareTo(range[0]) >= 0 && map.getGenericValue(field).compareTo(range[1]) <= 0;
        }
        // if the filter is equal to current field, return true
        else if (filter.equals(map.getGenericValue(field))) {
            return true;
        }
        // else check if the filter is "*"
        else {
            return filter.equals(filterAll);
        }
    }

    /**
     * Closes the stream object.
     * @throws Exception Throws generic exception.
     */
    public void closeStream() throws Exception {
        if (this.sortObj != null) {
            this.sortObj.close();
        }
        if (mapScan != null) {
            mapScan.closescan();
        }
        for (int i=0; i<5; i++) {
            if (this.btreeScanners[i] != null) {
                this.btreeScanners[i].DestroyBTreeFileScan();
            }
        }

    }

    /**
     * @return returns the Map in the Stream based on query conditions.
     * @throws Exception throws generic exception.
     */
    public Map getNext() throws Exception {
        if (this.sortObj == null) {
            System.out.println("sort object is not initialised");
            return null;
        }
        Map m = null;
        try {
            m = this.sortObj.get_next();

        } catch (OutOfSpaceException e) {
            e.printStackTrace();
            closeStream();
        }
        if (m == null) {
            tempHeapFile.deleteFile();
            closeStream();
            return null;
        }
        return m;
    }
}