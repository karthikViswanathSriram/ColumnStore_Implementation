package columnar;

import bitmap.BM;
import bitmap.BitMapFile;
import btree.*;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.*;

public class Columnarfile {

    // Number of columns
    short numColumns;

    // Types of columns
    AttrType[] attrTypes = null;

    // Size of the Attributes
    short[] attrSize;

    // Actual size for string including the addition 2 bytes
    short[] actualSize;

    // Column heal files
    private Heapfile[] heapFiles = null;

    // Table name
    String tableName = null;

    // Map Column Name to ID
    HashMap<String, Integer> columnMap;

    // Map BTREE Index name to object
    HashMap<String, BTreeFile> BTMap;

    // Map BITMAP name to object
    HashMap<String, BitMapFile> BMMap;

    /**
     * Opens an existing columnar file
     *
     * @param name of the columarfile
     * @throws HFException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws IOException
     */
    public Columnarfile(java.lang.String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        Heapfile tempFile = null;
        Scan scan = null;
        tableName = name;
        columnMap = new HashMap<>();
        try {
            // get the columnar header page
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(name + ".hdr");
            if (pid == null) {
                throw new Exception("Columnar with the name: " + name + ".hdr doesn't exists");
            }

            tempFile = new Heapfile(name + ".hdr");

            scan = tempFile.openScan();
            RID hdrRid = new RID();
            Tuple hdr = scan.getNext(hdrRid);
            hdr.setHeaderMetaData();

            // Header contains number of columns, type 1, size 1, columnName 1, ...., type
            // n, size n , columnName n

            this.numColumns = (short) hdr.getIntFld(1);
            attrTypes = new AttrType[numColumns];
            attrSize = new short[numColumns];
            actualSize = new short[numColumns];
            heapFiles = new Heapfile[numColumns];
            int j = 0;
            for (int i = 0; i < numColumns; i++, j = j + 3) {
                attrTypes[i] = new AttrType(hdr.getIntFld(2 + j));
                attrSize[i] = (short) hdr.getIntFld(3 + j);
                String colName = hdr.getStrFld(4 + j);
                columnMap.put(colName, i);
                actualSize[i] = attrSize[i];
                if (attrTypes[i].attrType == AttrType.attrString)
                    actualSize[i] += 2;
            }
            BTMap = new HashMap<>();
            BMMap = new HashMap<>();

            // create a idx file to store all column which consists of indexes
            pid = SystemDefs.JavabaseDB.get_file_entry(name + ".idx");
            if (pid != null) {
                tempFile = new Heapfile(name + ".idx");
                scan = tempFile.openScan();
                RID r = new RID();
                Tuple t = scan.getNext(r);
                while (t != null) {
                    t.setHeaderMetaData();
                    int indexType = t.getIntFld(1);
                    if (indexType == 0)
                        BTMap.put(t.getStrFld(2), null);
                    else if (indexType == 1)
                        BMMap.put(t.getStrFld(2), null);
                    t = scan.getNext(r);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates a new columnar file
     *
     * @param name
     * @param numcols
     * @param types
     * @param attrSizes
     * @param colnames
     * @throws IOException
     * @throws InvalidTupleSizeException
     * @throws InvalidTypeException
     * @throws FieldNumberOutOfBoundException
     * @throws SpaceNotAvailableException
     * @throws HFException
     * @throws HFBufMgrException
     * @throws InvalidSlotNumberException
     * @throws HFDiskMgrException
     */
    public Columnarfile(java.lang.String name, int numcols, AttrType[] types, short[] attrSizes, String[] colnames)
            throws IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException,
            SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
        Heapfile hdrFile = null;
        columnMap = new HashMap<>();
        try {
            heapFiles = new Heapfile[numcols];
            hdrFile = new Heapfile(name + ".hdr");

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        numColumns = (short) (numcols);
        this.tableName = name;
        attrTypes = new AttrType[numColumns];
        attrSize = new short[numColumns];
        actualSize = new short[numColumns];
        int k = 0;
        for (int i = 0; i < numcols; i++) {
            attrTypes[i] = new AttrType(types[i].attrType);
            switch (types[i].attrType) {
                case 0:
                    actualSize[i] = attrSize[i] = attrSizes[k];
                    actualSize[i] += 2;
                    k++;
                    break;
                case 1:
                case 2:
                    actualSize[i] = attrSize[i] = 4;
                    break;
                case 3:
                    actualSize[i] = attrSize[i] = 1;
                    break;
                case 4:
                    attrSize[i] = 0;
                    break;
            }
        }

        AttrType[] htypes = new AttrType[2 + (numcols * 3)];
        htypes[0] = new AttrType(AttrType.attrInteger);
        for (int i = 1; i < htypes.length - 1; i = i + 3) {
            htypes[i] = new AttrType(AttrType.attrInteger);
            htypes[i + 1] = new AttrType(AttrType.attrInteger);
            htypes[i + 2] = new AttrType(AttrType.attrString);
        }
        htypes[htypes.length - 1] = new AttrType(AttrType.attrInteger);
        short[] hsizes = new short[numcols];
        for (int i = 0; i < numcols; i++) {
            hsizes[i] = 20; // column name can't be more than 20 chars
        }
        Tuple hdr = new Tuple();
        hdr.setHdr((short) htypes.length, htypes, hsizes);
        int size = hdr.size();

        hdr = new Tuple(size);
        hdr.setHdr((short) htypes.length, htypes, hsizes);
        hdr.setIntFld(1, numcols);
        int j = 0;
        for (int i = 0; i < numcols; i++, j = j + 3) {
            hdr.setIntFld(2 + j, attrTypes[i].attrType);
            hdr.setIntFld(3 + j, attrSize[i]);
            hdr.setStrFld(4 + j, colnames[i]);
            columnMap.put(colnames[i], i);
        }
        hdrFile.insertRecord(hdr.returnTupleByteArray());
        BTMap = new HashMap<>();
        BMMap = new HashMap<>();

    }

    /**
     *
     * @throws InvalidSlotNumberException
     * @throws FileAlreadyDeletedException
     * @throws InvalidTupleSizeException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws IOException
     * @throws HFException
     */
    public void deleteColumnarFile() throws InvalidSlotNumberException, FileAlreadyDeletedException,
            InvalidTupleSizeException, HFBufMgrException, HFDiskMgrException, IOException, HFException {
        for (int i = 0; i < numColumns; i++) {
            heapFiles[i].deleteFile();
        }
        Heapfile hdr = new Heapfile(tableName + "hdr");
        hdr.deleteFile();
        Heapfile idx = new Heapfile(tableName + "idx");
        idx.deleteFile();
        heapFiles = null;
        attrTypes = null;
        tableName = null;
        numColumns = 0;
    }

    // Assumption: tuple pointer contains header information.

    /**
     *
     * @param tuplePtr - insert the tuple and return the TID for the user
     * @return
     * @throws Exception
     */
    public TID insertTuple(byte[] tuplePtr) throws Exception {

        int offset = getOffset();
        RID[] rids = new RID[numColumns];
        int position = 0;
        for (int i = 0; i < numColumns; i++) {

            byte[] data = new byte[actualSize[i]];
            System.arraycopy(tuplePtr, offset, data, 0, actualSize[i]);
            rids[i] = getColumn(i).insertRecord(data);
            offset += actualSize[i];

            // update the indexes accordingly
            String btIndexname = getBTName(i);
            ValueClass val = attrTypes[i].attrType == AttrType.attrString
                    ? new StringValue(Convert.getStrValue(0, data, actualSize[i]))
                    : new IntegerValue(Convert.getIntValue(0, data));
            String bmIndexname = getBMName(i, val);
            if (BTMap != null && BTMap.containsKey(btIndexname)) {
                position = getColumn(i).positionOfRecord(rids[i]);
                getBTIndex(btIndexname).insert(KeyFactory.getKeyClass(data, attrTypes[i], actualSize[i]), position);
            }
            if (BMMap != null && BMMap.containsKey(bmIndexname)) {
                position = getColumn(i).positionOfRecord(rids[i]);
                getBMIndex(bmIndexname).Insert(position);
            }
            if (i + 1 == numColumns) {
                position = getColumn(1).positionOfRecord(rids[1]);
            }
        }
        TID tid = new TID(numColumns, position, rids);
        return tid;
    }

    /**
     * Returns a Tuple for a given TID
     *
     * @param tid
     * @return
     * @throws Exception
     */
    public Tuple getTuple(TID tid) throws Exception {

        Tuple result = new Tuple(getTupleSize());
        result.setHdr(numColumns, attrTypes, getStrSize());
        byte[] data = result.getTupleByteArray();
        int offset = getOffset();
        for (int i = 0; i < numColumns; i++) {
            Tuple t = getColumn(i).getRecord(tid.recordIDs[i]);
            System.arraycopy(t.getTupleByteArray(), 0, data, offset, actualSize[i]);
            offset += actualSize[i];
        }

        result.tupleInit(data, 0, data.length);

        return result;
    }

    /**
     * get the value for tidarg and respective column
     *
     * @param tid
     * @param column
     * @return
     * @throws Exception
     */
    public ValueClass getValue(TID tid, int column) throws Exception {

        Tuple t = getColumn(column).getRecord(tid.recordIDs[column]);
        return attrTypes[column].attrType == AttrType.attrString
                ? new StringValue(Convert.getStrValue(0, t.getTupleByteArray(), actualSize[column]))
                : new IntegerValue(Convert.getIntValue(0, t.getTupleByteArray()));
    }

    /**
     *
     * @return
     * @throws HFDiskMgrException
     * @throws HFException
     * @throws HFBufMgrException
     * @throws IOException
     * @throws InvalidTupleSizeException
     * @throws InvalidSlotNumberException
     */
    public int getTupleCnt() throws HFDiskMgrException, HFException,
            HFBufMgrException, IOException, InvalidTupleSizeException, InvalidSlotNumberException {
        return getColumn(0).getRecCnt();
    }

    /**
     * opens the tuple scan on all the columns by open all the heap files
     *
     * @return
     * @throws Exception
     */
    public TupleScan openTupleScan() throws Exception {

        return new TupleScan(this);
    }

    /**
     * opens tuple scan on the given columns
     *
     * @param columns
     * @return
     * @throws Exception
     */
    public TupleScan openTupleScan(short[] columns) throws Exception {
        return new TupleScan(this, columns);
    }

    /**
     * opens tuple scan for the given column
     * 
     * @param columnNo
     * @return
     * @throws Exception
     */
    public Scan openColumnScan(int columnNo) throws Exception {
        if (columnNo < heapFiles.length) {
            return new Scan(getColumn(columnNo));
        } else {

            throw new Exception("Invalid Column number");
        }

    }

    /**
     * updates the tuple with given newTuple for the TID argument
     *
     * @param tidarg
     * @param newtuple
     * @return
     */
    public boolean updateTuple(TID tidarg, Tuple newtuple) {
        try {

            int offset = getOffset();
            byte[] tupleData = newtuple.getTupleByteArray();
            for (int i = 0; i < numColumns; i++) {

                byte[] data = new byte[actualSize[i]];
                System.arraycopy(tupleData, offset, data, 0, actualSize[i]);
                Tuple t = new Tuple(actualSize[i]);
                t.tupleInit(data, 0, data.length);
                getColumn(i).updateRecord(tidarg.recordIDs[i], t);
                offset += actualSize[i];
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     *
     * @param tid
     * @param newtuple
     * @param column
     * @return
     */
    public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column) {
        try {
            int offset = getOffset(column);
            byte[] tupleData = newtuple.getTupleByteArray();
            Tuple t = new Tuple(actualSize[column]);
            byte[] data = t.getTupleByteArray();
            System.arraycopy(tupleData, offset, data, 0, actualSize[column]);
            t.tupleInit(data, 0, data.length);
            getColumn(column).updateRecord(tid.recordIDs[column], t);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     *
     * @param columnNo
     * @return
     * @throws Exception
     */
    public boolean createBTreeIndex(int columnNo) throws Exception {
        String btreeName = getBTName(columnNo);

        BTreeFile bTreeFile = new BTreeFile(btreeName, attrTypes[columnNo].attrType, actualSize[columnNo],
                DeleteFashion.NAIVE_DELETE);
        Scan columnScan = openColumnScan(columnNo);
        RID rid = new RID();
        Tuple tuple;
        while (true) {
            tuple = columnScan.getNext(rid);
            if (tuple == null) {
                break;
            }
            int pos = getColumn(columnNo).positionOfRecord(rid);
            bTreeFile.insert(
                    KeyFactory.getKeyClass(tuple.getTupleByteArray(), attrTypes[columnNo], actualSize[columnNo]), pos);
        }
        columnScan.closescan();
        addIndexToColumnar(0, btreeName);
        return true;
    }

    /**
     *
     * @param columnNo
     * @param value
     * @return
     * @throws Exception
     */
    public boolean createBitMapIndex(int columnNo, ValueClass value) throws Exception {

        short[] targetedCols = new short[1];
        targetedCols[0] = (short) columnNo;

        FldSpec[] proj = new FldSpec[1];
        proj[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        ColumnarColumnScan columnScan = new ColumnarColumnScan(getColumnarFileName(), columnNo,
                proj,
                targetedCols,
                null, null);

        String indexName = getBMName(columnNo, value);
        BitMapFile bitMapFile = new BitMapFile(indexName, this, columnNo, value, attrTypes[columnNo]);
        Tuple tuple;
        int pos = 0;
        while (true) {
            tuple = columnScan.get_next();

            if (tuple == null) {
                break;
            }

            short[] fldOff = tuple.copyFldOffset();
            ValueClass valueClass = attrTypes[columnNo].attrType == AttrType.attrString
                    ? new StringValue(Convert.getStrValue(fldOff[0], tuple.getTupleByteArray(), actualSize[columnNo]))
                    : new IntegerValue(Convert.getIntValue(fldOff[0], tuple.getTupleByteArray()));

            if (valueClass.toString().equals(value.toString())) {
                bitMapFile.Insert(pos);
            } else {
                bitMapFile.Delete(pos);
            }
            pos++;
        }
        columnScan.close();
        bitMapFile.close();

        addIndexToColumnar(1, indexName);

        return true;
    }

    /**
     *
     * @param columnNo
     * @return
     * @throws Exception
     */
    public boolean createAllBitMapIndexForColumn(int columnNo) throws Exception {
        short[] targetedCols = new short[1];
        targetedCols[0] = (short) columnNo;

        FldSpec[] proj = new FldSpec[1];
        proj[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        ColumnarColumnScan columnScan = new ColumnarColumnScan(getColumnarFileName(), columnNo,
                proj,
                targetedCols,
                null, null);

        RID rid = new RID();
        Tuple tuple;
        int pos = 0;
        Set<BitMapFile> bitMapFiles = new HashSet<>();
        while (true) {
            tuple = columnScan.get_next();
            if (tuple == null) {
                break;
            }

            ValueClass valueClass = attrTypes[columnNo].attrType == AttrType.attrInteger
                    ? new IntegerValue(tuple.getIntFld(1))
                    : new StringValue(tuple.getStrFld(1));

            BitMapFile bitMapFile;
            String bitMapFileName = getBMName(columnNo, valueClass);
            if (!BMMap.containsKey(bitMapFileName)) {
                bitMapFile = new BitMapFile(bitMapFileName, this, columnNo, valueClass, attrTypes[columnNo]);
                addIndexToColumnar(1, bitMapFileName);
                BMMap.put(bitMapFileName, bitMapFile);
            }else if(BMMap.get(bitMapFileName) == null) {
                BMMap.put(bitMapFileName, new BitMapFile(bitMapFileName, this, columnNo, valueClass, attrTypes[columnNo]));
            }
            bitMapFile = BMMap.get(bitMapFileName);
            bitMapFiles.add(bitMapFile);

            for (BitMapFile existingBitMapFile : bitMapFiles) {
                if (existingBitMapFile.getHeaderPage().getValue().equals(valueClass.toString())) {
                    existingBitMapFile.Insert(pos);
                } else {
                    existingBitMapFile.Delete(pos);
                }
            }

            pos++;
        }
        columnScan.close();
        
        for (BitMapFile bitMapFile : bitMapFiles) {
            bitMapFile.close();
        }
        
        
        
        return true;
    }

    /**
     * Marks all records at position as deleted. Scan skips over these records
     *
     * @param pos
     * @return
     */
    public boolean markTupleDeleted(int pos) {
        String fileName = getDeletedFileName();
        try {
            Heapfile tempFile = new Heapfile(fileName);
            AttrType[] types = new AttrType[1];
            types[0] = new AttrType(AttrType.attrInteger);
            short[] sizes = new short[0];
            Tuple t = new Tuple(10);
            t.setHdr((short) 1, types, sizes);
            t.setIntFld(1, pos);
            tempFile.insertRecord(t.getTupleByteArray());

            for (int i = 0; i < numColumns; i++) {
                Tuple tuple = getColumn(i).getRecord(pos);
                ValueClass valueClass = attrTypes[i].attrType == 0
                        ? new StringValue(Convert.getStrValue(0, tuple.getTupleByteArray(), actualSize[i]))
                        : new IntegerValue(Convert.getIntValue(0, tuple.getTupleByteArray()));
                ;
                KeyClass keyClass = KeyFactory.getKeyClass(tuple.getTupleByteArray(),
                        attrTypes[i],
                        actualSize[i]);

                String bTreeFileName = getBTName(i);
                String bitMapFileName = getBMName(i, valueClass);
                if (BTMap.containsKey(bTreeFileName)) {
                    BTreeFile bTreeFile = getBTIndex(bTreeFileName);
                    bTreeFile.Delete(keyClass, pos);
                }
                if (BMMap.containsKey(bitMapFileName)) {
                    BitMapFile bitMapFile = getBMIndex(bitMapFileName);
                    bitMapFile.Delete(pos);
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     *
     * @param tid
     * @return
     */
    public boolean markTupleDeleted(TID tid) {
        return markTupleDeleted(tid.position);
    }

    /**
     * Purges all tuples marked for deletion. Removes keys/positions from indexes
     * too
     *
     * @return
     * @throws HFDiskMgrException
     * @throws InvalidTupleSizeException
     * @throws IOException
     * @throws InvalidSlotNumberException
     * @throws FileAlreadyDeletedException
     * @throws HFBufMgrException
     * @throws SortException
     */
    public boolean purgeAllDeletedTuples() throws HFDiskMgrException, InvalidTupleSizeException, IOException,
            InvalidSlotNumberException, FileAlreadyDeletedException, HFBufMgrException, SortException {

        Sort deletedTuples = null;
        RID rid;
        Heapfile f = null;
        boolean done = false;
        try {
            f = new Heapfile(getDeletedFileName());
        } catch (Exception e) {
            System.err.println(" Could not open heapfile");
            e.printStackTrace();
            f.deleteFile();
            return false;
        }

        try {
            AttrType[] types = new AttrType[1];
            types[0] = new AttrType(AttrType.attrInteger);
            short[] sizes = new short[0];
            FldSpec[] proj = new FldSpec[1];
            proj[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
            FileScan fileScan = new FileScan(getDeletedFileName(), types, sizes, (short) 1, 1, proj, null);
            deletedTuples = new Sort(types, (short) 1, sizes, fileScan, 1, new TupleOrder(TupleOrder.Descending), 4,
                    10);

        } catch (Exception e) {
            System.err.println("*** Error opening scan\n");
            e.printStackTrace();
            f.deleteFile();
            return false;
        }

        Tuple tuple;
        while (!done) {
            try {
                rid = new RID();
                tuple = deletedTuples.get_next();
                if (tuple == null) {
                    deletedTuples.close();
                    done = true;
                    break;
                }
                int markedPos = Convert.getIntValue(6, tuple.getTupleByteArray());
                for (int j = 0; j < numColumns; j++) {
                    rid = getColumn(j).recordAtPosition(markedPos);
                    getColumn(j).deleteRecord(rid);

                    for (String fileName : BMMap.keySet()) {
                        int columnNo = Integer.parseInt(fileName.split("\\.")[2]);
                        if (columnNo == 0) {
                            BitMapFile bitMapFile = getBMIndex(fileName);
                            bitMapFile.fullDelete(markedPos);
                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
                if (deletedTuples != null)
                    deletedTuples.close();
                f.deleteFile();
                return false;
            }
        }
        f.deleteFile();

        return true;
    }

    /**
     * Write the indexes created on each column to the .idx file
     *
     * @param indexType
     * @param indexName
     * @return
     */
    private boolean addIndexToColumnar(int indexType, String indexName) {

        try {
            AttrType[] types = new AttrType[2];
            types[0] = new AttrType(AttrType.attrInteger);
            types[1] = new AttrType(AttrType.attrString);
            short[] isizes = new short[1];
            isizes[0] = 40; // index name can't be more than 40 chars
            Tuple t = new Tuple();
            t.setHdr((short) 2, types, isizes);
            int size = t.size();
            t = new Tuple(size);
            t.setHdr((short) 2, types, isizes);
            t.setIntFld(1, indexType);
            t.setStrFld(2, indexName);
            Heapfile f = new Heapfile(tableName + ".idx");
            f.insertRecord(t.getTupleByteArray());

            if (indexType == 0) {
                BTMap.put(indexName, null);
            } else if (indexType == 1) {
                BMMap.put(indexName, null);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Return the respective column heap file
     *
     * @param columnNo
     * @return
     * @throws IOException
     * @throws HFException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     */
    public Heapfile getColumn(int columnNo) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        return heapFiles[columnNo] == null ? new Heapfile(tableName + columnNo) : heapFiles[columnNo];
    }

    /**
     * return the BTree index for the given indexName
     *
     * @param indexName
     * @return
     * @throws IOException
     * @throws HFException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws ConstructPageException
     * @throws GetFileEntryException
     * @throws PinPageException
     */
    public BTreeFile getBTIndex(String indexName) throws IOException, HFException, HFBufMgrException,
            HFDiskMgrException, ConstructPageException, GetFileEntryException, PinPageException {
        if (!BTMap.containsKey(indexName))
            return null;
        if (BTMap.get(indexName) == null)
            BTMap.put(indexName, new BTreeFile(indexName));

        return BTMap.get(indexName);
    }

    /**
     * Return bitmap file for the given indexName
     *
     * @param indexName
     * @return
     * @throws Exception
     */
    public BitMapFile getBMIndex(String indexName) throws Exception {
        if (!BMMap.containsKey(indexName))
            return null;
        if (BMMap.get(indexName) == null)
            BMMap.put(indexName, new BitMapFile(indexName));

        return BMMap.get(indexName);
    }

    /**
     * remove all the dangling files for the store
     */
    public void close() {
        if (heapFiles != null) {
            for (int i = 0; i < heapFiles.length; i++)
                heapFiles[i] = null;
        }
        try {
            if (BTMap != null) {
                for (BTreeFile bt : BTMap.values()) {
                    if (bt != null) {
                        bt.close();
                    }
                }
            }
            if (BMMap != null) {
                for (BitMapFile bm : BMMap.values()) {
                    if (bm != null) {
                        bm.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error closing columnar: " + tableName);
        }
    }

    public int getTupleSize() {

        int size = getOffset();
        for (int i = 0; i < numColumns; i++) {
            size += actualSize[i];
        }
        return size;
    }

    public short[] getStrSize() {

        int n = 0;
        for (int i = 0; i < numColumns; i++) {
            if (attrTypes[i].attrType == AttrType.attrString)
                n++;
        }

        short[] strSize = new short[n];
        int count = 0;
        for (int i = 0; i < numColumns; i++) {
            if (attrTypes[i].attrType == AttrType.attrString) {
                strSize[count++] = attrSize[i];
            }
        }
        return strSize;
    }

    public short[] getStrSize(short[] targetColumns) {

        int n = 0;
        for (int i = 0; i < targetColumns.length; i++) {
            if (attrTypes[targetColumns[i]].attrType == AttrType.attrString)
                n++;
        }

        short[] strSize = new short[n];
        int count = 0;
        for (int i = 0; i < targetColumns.length; i++) {
            if (attrTypes[targetColumns[i]].attrType == AttrType.attrString) {
                strSize[count++] = attrSize[targetColumns[i]];
            }
        }

        return strSize;
    }

    public int getOffset() {
        return 2 * (numColumns + 2);
    }

    public int getOffset(int column) {
        int offset = 4 + (numColumns * 2);
        for (int i = 0; i < column; i++) {
            offset += actualSize[i];
        }
        return offset;
    }

    public String getColumnarFileName() {
        return tableName;
    }

    public AttrType[] getAttributes() {
        return attrTypes;
    }

    public short[] getAttrSizes() {
        return attrSize;
    }

    public int getAttributePosition(String name) {
        return columnMap.get(name);
    }

    /**
     * return the BT Name
     *
     * @param columnNo
     * @return
     */
    public String getBTName(int columnNo) {
        return "BT" + "." + tableName + "." + columnNo;
    }

    /**
     * return the BitMap file name by following the conventions
     *
     * @param columnNo
     * @param value
     * @return
     */
    public String getBMName(int columnNo, ValueClass value) {
        return "BM" + "." + tableName + "." + columnNo + "." + value.toString();
    }

    public String[] getAvailableBM(int columnNo) {
        List<String> bmName = new ArrayList<>();
        String prefix = "BM" + "." + tableName + "." + columnNo + ".";
        for (String s : BMMap.keySet()) {
            if (s.substring(0, prefix.length()).equals(prefix)) {
                bmName.add(s);
            }
        }
        return bmName.toArray(new String[bmName.size()]);
    }

    public String getDeletedFileName() {
        return tableName + ".del";
    }

    public short getnumColumns() {
        return numColumns;
    }

    /**
     * given a column returns the AttrType
     *
     * @param columnNo
     * @return
     * @throws Exception
     */
    public AttrType getAttrtypeforcolumn(int columnNo) throws Exception {
        if (columnNo < numColumns) {
            return attrTypes[columnNo];
        } else {
            throw new Exception("Invalid Column Number");
        }
    }

    /**
     * given the column returns the size of AttrString
     *
     * @param columnNo
     * @return
     * @throws Exception
     */
    public short getAttrsizeforcolumn(int columnNo) throws Exception {
        if (columnNo < numColumns) {
            return attrSize[columnNo];
        } else {
            throw new Exception("Invalid Column Number");
        }
    }

    // phase 3

    public HashMap<String, BitMapFile> getAllBitMaps() {
        return BMMap;
    }

    public Tuple getTuple(int position) throws Exception {

        for (int i = 0; i < heapFiles.length; i++) {
            heapFiles[i] = new Heapfile(getColumnarFileName() + i);
        }
        Tuple tuple = new Tuple();
        // set the header which attribute types of the targeted columns
        tuple.setHdr((short) heapFiles.length, attrTypes, getStrSize());

        tuple = new Tuple(tuple.size());
        tuple.setHdr((short) heapFiles.length, attrTypes, getStrSize());
        for (int i = 0; i < heapFiles.length; i++) {
            RID rid = heapFiles[i].recordAtPosition(position);
            Tuple record = heapFiles[i].getRecord(rid);
            switch (attrTypes[i].attrType) {
                case AttrType.attrInteger:
                    // Assumed that column heap page will have only one entry
                    tuple.setIntFld(i + 1,
                            Convert.getIntValue(0, record.getTupleByteArray()));
                    break;
                case AttrType.attrString:
                    tuple.setStrFld(i + 1,
                            Convert.getStrValue(0, record.getTupleByteArray(), attrSize[i] + 2));
                    break;
                default:
                    throw new Exception("Attribute indexAttrType not supported");
            }
        }

        return tuple;
    }

}
