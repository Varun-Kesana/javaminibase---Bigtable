package BigT;

import btree.*;
import bufmgr.*;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import starter.BigTableMain;
import global.*;
import heap.*;
import iterator.MapUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

class MapLocation implements Serializable {
    MID mid;
    int heapIndex;

    MapLocation(MID mid, int heapIndex) {
        this.mid = mid;
        this.heapIndex = heapIndex;
    }
}
public class bigT {

    // Name of the BigT file
    String tableName;

    // For each heapfile we have 5 index files
    /**
     *
     * [idx5  idx5  idx5  idx5  idx5]
     * [idx4  idx4  idx4  idx4  idx4]
     * [idx3  idx3  idx3  idx3  idx3]
     * [idx2  idx2  idx2  idx2  idx2]
     * [idx1  idx1  idx1  idx1  idx1]
     * [heap1 heap2 heap3 heap4 heap5]
     */
    BTreeFile[][] indexFiles;
    Heapfile[] heapfiles;

    // HashMap used for maintaining different (3) map versions
    java.util.Map<String, List<MapLocation>> mapVersion;

    static String splitRegex = ",";
    public static int insertions;
    public static int deletions;


    /**
     * Returns the bigtable instance if it already exists, else creates a new bigtable
     *
     * @param tableName - Name of the table
     */
    public bigT(String tableName) {
        try {
            boolean tableExists = false;
            this.tableName = tableName;
            this.indexFiles = new BTreeFile[5][5];
            this.heapfiles = new Heapfile[5];
            insertions = 0;
            deletions = 0;

            PageId heapFilePageId = SystemDefs.JavabaseDB.get_file_entry(tableName  + ".1.heap");
            if (heapFilePageId != null) {
                tableExists = true;
            }

            for (int i=0; i<5; i++) {
                heapfiles[i] = new Heapfile(tableName + "." + (i+1) + ".heap");
            }

            // Initialize the HashMap used for maintaining versions
            try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream("/tmp/" + this.tableName + ".hashmap.ser"))) {
                this.mapVersion = (HashMap<String, List<MapLocation>>) objectInputStream.readObject();
            } catch (IOException e) {
                this.mapVersion = new HashMap<>();
            }

            // create and initialize the index files
            if (!tableExists) {
                createIndexes();
            } else {
                loadIndex();
            }
        } catch (Exception e) {
            System.out.println("From bigT(filename, type)");
            e.printStackTrace();
        }
    }

    /**
     *
     * @return number of maps in the bigtable
     */
    public int getMapCnt() throws HFBufMgrException, IOException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException {
        int count = 0;
        for (int i=0; i<5; i++) {
            count += heapfiles[i].getRecCnt();
        }

        return count;
    }

    /**
     *
     * @return number of distinct row labels in the bigtable
     */
    public int getRowCnt() {
        Set<String> rowSet = new HashSet<>();
        mapVersion.keySet().forEach(row -> rowSet.add(row.split(splitRegex)[0]));
        return rowSet.size();
    }

    /**
     *
     * @return number of distinct column labels in the bigtable
     */
    public int getColumnCnt() {
        Set<String> colSet = new HashSet<>();
        mapVersion.keySet().forEach(row -> colSet.add(row.split(splitRegex)[1]));
        return colSet.size();
    }

    public void printDistributedHeaps() throws HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException, IOException {
        for (int i=0; i<5; i++) {
            System.out.print("Storage Type " + (i+1) + ": " + heapfiles[i].getRecCnt());
            System.out.println();
        }
        System.out.print("**********************");
        System.out.print("\n");
    }

    public boolean indexExists(String indexFileName) throws InvalidPageNumberException, IOException, FileIOException, DiskMgrException {
        PageId pageId = SystemDefs.JavabaseDB.get_file_entry(indexFileName);
        return pageId != null;
    }

    /**
     * Inserts map into the big table and return its Mid.
     * At any given time at most 3 versions of map with same name and column but different timestamp are maintained
     * @return mid
     */
    public MID insertMap(byte[] mapPtr, int indexType) throws Exception {
        // map to be inserted
        Map map = new Map();
        map.setData(mapPtr);

        String key;
        String mapVersionKey = map.getRowLabel() + GlobalConst.DELIMITER + map.getColumnLabel();
        // list of MIDs of map versions that are stored currently
        List<MapLocation> mapVersionlist = mapVersion.get(mapVersionKey);
        boolean flag = false;

        if (mapVersionlist == null) {
            mapVersionlist = new ArrayList<>();
        } else {
            int oldestTimestamp = Integer.MAX_VALUE;
            MID oldestMID = null;
            Map oldestMap = new Map();
            int oldestHeapIndex = -1;

            // there can't be more than 3 map versions
            if (mapVersionlist.size() > 3) {
                throw new IOException("database file is corrupted.");
            }

            // if the number of map versions are 3 then we have to remove the oldest timestamp one
            // While removing map from heap remove it from all available index files
            if (mapVersionlist.size() <= 3) {
                // retrieve the Map for the given MID from heapfile

                for (MapLocation mapLocation : mapVersionlist) {
                    Map currMap = heapfiles[mapLocation.heapIndex].getMap(mapLocation.mid);

                    if (MapUtils.Equal(currMap, map)) {
                        return mapLocation.mid;
                    } else if (currMap.getRowLabel().equals(map.getRowLabel()) && currMap.getColumnLabel().equals(map.getColumnLabel()) && currMap.getTimeStamp() == map.getTimeStamp()) {
                        oldestTimestamp = currMap.getTimeStamp();
                        oldestMID = mapLocation.mid;
                        oldestMap = currMap;
                        oldestHeapIndex = mapLocation.heapIndex;
                        flag = true;
                        break;
                    } else {
                        if (mapVersionlist.size() == 3 && currMap.getTimeStamp() < oldestTimestamp) {
                            oldestTimestamp = currMap.getTimeStamp();
                            oldestMID = mapLocation.mid;
                            oldestMap = currMap;
                            oldestHeapIndex = mapLocation.heapIndex;
                        }
                    }
                }
            }
            // if the map to be inserted is older than the oldest map versions than we skip insertion
            if (mapVersionlist.size() == 3 && map.getTimeStamp() < oldestTimestamp) {
                return oldestMID;
            }

            // if we have 3 versions already we have to remove the oldest one to make room for new map
            if (flag || mapVersionlist.size() == 3) {
                // remove the oldest map from all the index files that are created
                for (int i=2; i<=5; i++) {
                    switch (i) {
                        case 2:
                            key = oldestMap.getRowLabel();
                            break;
                        case 3:
                            key = oldestMap.getColumnLabel();
                            break;
                        case 4:
                            key = oldestMap.getColumnLabel() + GlobalConst.DELIMITER + oldestMap.getRowLabel();
                            break;
                        case 5:
                            key = oldestMap.getRowLabel() + GlobalConst.DELIMITER + oldestMap.getValue();
                            break;
                        default:
                            throw new Exception("Invalid Index Type");
                    }
                    if (this.indexFiles[oldestHeapIndex][i-1] != null) {
                        deletions++;
                        this.indexFiles[oldestHeapIndex][i-1].Delete(new StringKey(key), MapUtils.copy_mid_to_rid(oldestMID));
                    }
                }

                // remove the oldest map from heap
                heapfiles[oldestHeapIndex].deleteMap(oldestMID);
                removeObject(mapVersionlist, new MapLocation(oldestMID, oldestHeapIndex));
            } // end removal from index file
        } // end else block when mapVersionlist is non empty

        // now insert the map into the given heapfile
        MID mid = heapfiles[indexType-1].insertMap(mapPtr);
        RID rid = MapUtils.copy_mid_to_rid(mid);
        mapVersionlist.add(new MapLocation(mid, indexType-1));
        mapVersion.put(mapVersionKey, mapVersionlist);

        for (int i=2; i<=5; i++) {
            // insert new map into index files
            switch (i) {
                case 2:
                    key = map.getRowLabel();
                    break;
                case 3:
                    key = map.getColumnLabel();
                    break;
                case 4:
                    key = map.getColumnLabel() + GlobalConst.DELIMITER + map.getRowLabel();
                    break;
                case 5:
                    key = map.getRowLabel() + GlobalConst.DELIMITER + map.getValue();
                    break;
                default:
                    throw new Exception("Invalid Index Type");
            }
            if (this.indexFiles[indexType-1][i-1] != null) {
                insertions++;
                this.indexFiles[indexType-1][i-1].insert(new StringKey(key), rid);
            }
        }
        return mid;
    }

    /**
     * Removes mapLocation object from mapVersionlist
     * @param mapVersionlist
     * @param mapLocation
     */
    private void removeObject(List<MapLocation> mapVersionlist, MapLocation mapLocation) {
        int idx = -1;
        for (int i=0; i<mapVersionlist.size(); i++) {
            MapLocation mapLocation1 = mapVersionlist.get(i);
            if (mapLocation.mid.equals(mapLocation1.mid) && (mapLocation.heapIndex == mapLocation1.heapIndex)) {
                idx = i;
                break;
            }
        }
        // TODO: throw error if idx = -1, idx cannot be -1
        mapVersionlist.remove(idx);
    }


    /**
     *
     * Opens and returns a stream of map for querying
     * @param orderType
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     * @return new stream instance
     * @throws Exception
     */
    public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
    }



    /**
     * Creates btree index files and returns the instance of the files in variable
     */
    private void createIndexes() throws Exception {
        // Heap 1 index files
        indexFiles[0][0] = null;

        // Heap 2 index files
        indexFiles[1][1] = new BTreeFile(
                this.tableName + ".heap2.index2.idx",
                AttrType.attrString,
                BigTableMain.BIGT_STR_SIZES[0],
                DeleteFashion.NAIVE_DELETE
        );

        // Heap 3 index files
        indexFiles[2][2] = new BTreeFile(
                this.tableName + ".heap3.index3.idx",
                AttrType.attrString,
                BigTableMain.BIGT_STR_SIZES[1],
                DeleteFashion.NAIVE_DELETE
        );

        // Heap 4 index files
        indexFiles[3][3] = new BTreeFile(
                this.tableName + ".heap4.index4.idx",
                AttrType.attrString,
                BigTableMain.BIGT_STR_SIZES[0] + BigTableMain.BIGT_STR_SIZES[1] + GlobalConst.DELIMITER.getBytes().length,
                DeleteFashion.NAIVE_DELETE
        );

        // Heap 5 index files
        indexFiles[4][4] = new BTreeFile(
                this.tableName + ".heap5.index5.idx",
                AttrType.attrString,
                BigTableMain.BIGT_STR_SIZES[0] + BigTableMain.BIGT_STR_SIZES[2] + GlobalConst.DELIMITER.getBytes().length,
                DeleteFashion.NAIVE_DELETE
        );
    }

    /**
     * Create an new index on given heapfile and insert all the data into the index (sync)
     * @param indexType
     */
    public void createAndSyncIndex(int heapIndex, int indexType) throws Exception {
        String key;
        int keySize;
        switch (indexType) {
            case 2:
                keySize = BigTableMain.BIGT_STR_SIZES[0];
                break;
            case 3:
                keySize = BigTableMain.BIGT_STR_SIZES[1];
                break;
            case 4:
                keySize = BigTableMain.BIGT_STR_SIZES[0] + BigTableMain.BIGT_STR_SIZES[1] + GlobalConst.DELIMITER.getBytes().length;
                break;
            case 5:
                keySize = BigTableMain.BIGT_STR_SIZES[0] + BigTableMain.BIGT_STR_SIZES[2] + GlobalConst.DELIMITER.getBytes().length;
                break;
            default:
                throw new Exception("Invalid Index Type");
        }

        // If the index already exists throw an error
        if (indexExists(this.tableName + ".heap" + heapIndex + ".index" + indexType + ".idx")) {
            System.out.println("The Index Type already exists, try creating a different one.");
            System.exit(0);
        }

        // create new index file
        indexFiles[heapIndex-1][indexType-1] = new BTreeFile(
                                            this.tableName + ".heap" + heapIndex + ".index" + indexType + ".idx",
                                            AttrType.attrString,
                                            keySize,
                                            DeleteFashion.NAIVE_DELETE
                                        );

        // transfer all data from heap to index file
        MID midObj = new MID();
        MapScan mapScan = heapfiles[heapIndex-1].openMapScan();
        Map map = mapScan.getNext(midObj);

        while (map != null) {
            switch (indexType) {
                case 2:
                    key = map.getRowLabel();
                    break;
                case 3:
                    key = map.getColumnLabel();
                    break;
                case 4:
                    key = map.getColumnLabel() + GlobalConst.DELIMITER + map.getRowLabel();
                    break;
                case 5:
                    key = map.getRowLabel() + GlobalConst.DELIMITER + map.getValue();
                    break;
                default:
                    throw new Exception("Invalid Index Type");
            }
            RID rid = MapUtils.copy_mid_to_rid(midObj);
            indexFiles[heapIndex-1][indexType-1].insert(new StringKey(key), rid);
            map = mapScan.getNext(midObj);
        }
    }

    /**
     * Opens and loads the btree files into variables
     */
    private void loadIndex() throws Exception {
        // check if each heap has any saved index files
        for (int heap=1; heap<=5; heap++) {
            for (int index=2; index<=5; index++) {
                if (indexExists(this.tableName + ".heap" + heap + ".index" + index + ".idx")) {
                    indexFiles[heap-1][index-1] = new BTreeFile(this.tableName + ".heap" + heap + ".index" + index + ".idx");
                }
            }
        }
    }

    /**
     * Closes the bigtable file and writes the mapversion into disk
     */
    public void close() throws PageUnpinnedException, PageNotFoundException, IOException, HashEntryNotFoundException, InvalidFrameNumberException, ReplacerException {
        for (int i=0; i<5; i++) {
            for (int j=0; j<5; j++) {
                if (this.indexFiles[i][j] != null) this.indexFiles[i][j].close();
            }
        }

        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream("/tmp/" + this.tableName + ".hashmap.ser"))) {
            objectOutputStream.writeObject(mapVersion);
        } catch (IOException e) {
            System.out.println("Exception from bigT.close");
            throw new IOException("Error while writing to file: " + e);
        }
    }
}