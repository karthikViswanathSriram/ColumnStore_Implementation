package columnar;

import java.io.IOException;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.InvalidUpdateException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import iterator.ColumnarColumnScan;
import iterator.FldSpec;
import iterator.RelSpec;
import btree.*;

import java.util.*;

import bitmap.BitMapFile;
import btree.BTreeFile;
import IntegerValue;
import StringValue;

public class Columnarfile {
    private int numColumns;
    AttrType[] type;
    private boolean _file_deleted;
    private Heapfile[] HF;
    private String CFname;
    private String[] BTfiles;
    private int strattrsize = 25;
    // Maps Attributes to position
    HashMap<String, Integer> columnMap;
    // Map to store the BTree indexes
    HashMap<String, BTreeFile> BTMap;
    // Map to store the BitMap indexes
    HashMap<String, BitMapFile> BMMap;

    /**
     * Opens existing columnar
     *
     * @param name of the columarfile
     * @throws HFException
     * @throws HFBufMgrException
     * @throws HFDiskMgrException
     * @throws IOException
     */
    public Columnarfile(java.lang.String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        Heapfile f = null;
        Scan scan = null;
        RID rid = null;
        CFname = name;
        try {
            // get the columnar header page
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(name + ".hdr");
            if (pid == null) {
                throw new Exception("Columnar with the name: " + name + ".hdr doesn't exists");
            }

            f = new Heapfile(name + ".hdr");

            // Header tuple is organized this way
            // NumColumns, AttrType1, AttrSize1, AttrName1, AttrType2, AttrSize2,
            // AttrName3...

            scan = f.openScan();
            RID hdrRid = new RID();
            Tuple hdr = scan.getNext(hdrRid);
            hdr.setHeaderMetaData();
            this.numColumns = (short) hdr.getIntFld(1);
            type = new AttrType[numColumns];
            HF = new Heapfile[numColumns];
            columnMap = new HashMap<>();
            int k = 0;
            for (int i = 0; i < numColumns; i++, k = k + 2) {
                type[i] = new AttrType(hdr.getIntFld(2 + k));
                String colName = hdr.getStrFld(3 + k);
                columnMap.put(colName, i);
            }
            BTMap = new HashMap<>();
            BMMap = new HashMap<>();

            // create a idx file to store all column which consists of indexes
            pid = SystemDefs.JavabaseDB.get_file_entry(name + ".idx");
            if (pid != null) {
                f = new Heapfile(name + ".idx");
                scan = f.openScan();
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

    public Columnarfile(java.lang.String name, int nColumns, AttrType[] type, String[] colNames)
            throws InvalidTypeException, InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException,
            InvalidSlotNumberException, SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException {
        columnMap = new HashMap<>();
        try {
            HF = new Heapfile[numColumns+1];
            HF[0] = new Heapfile(name + ".hdr");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        this.numColumns = nColumns;
        for (int column = 1; column < nColumns+1; column++) {
            try {
                HF[column] = new Heapfile(name + ".Col" + column);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.CFname = name;
        this.type = new AttrType[numColumns];
        this._file_deleted = false;
        this.BTfiles = new String[numColumns];
        Tuple temp = new Tuple();
        for(int i=0;i<numColumns;i++){
            this.type[i] = new AttrType(type[i].attrType);
            columnMap.put(colNames[i], i);
        }

        AttrType[] htypes = new AttrType[1 + (numColumns * 2)];
        htypes[0] = new AttrType(AttrType.attrInteger);
        for (int i = 1; i < htypes.length - 1; i = i + 2) {
            htypes[i] = new AttrType(AttrType.attrInteger);
            htypes[i + 1] = new AttrType(AttrType.attrString);
        }
        //htypes[htypes.length - 1] = new AttrType(AttrType.attrInteger);
        short[] hsizes = new short[numColumns];
        for (int i = 0; i < numColumns; i++) {
            hsizes[i] = 20; //column name can't be more than 20 chars
        }
        Tuple hdr = new Tuple();
        hdr.setHdr((short) htypes.length, htypes, hsizes);
        int size = hdr.size();

        hdr = new Tuple(size);
        hdr.setHdr((short) htypes.length, htypes, hsizes);
        hdr.setIntFld(1, numColumns);
        int j = 0;
        for (int i = 0; i < numColumns; i++, j = j + 2) {
            hdr.setIntFld(2 + j, type[i].attrType);
            hdr.setStrFld(3 + j, colNames[i]);
        }
        HF[0].insertRecord(hdr.returnTupleByteArray());
        BTMap = new HashMap<>();
        BMMap = new HashMap<>();
    }

    public void deleteColumnarFile() throws Exception {
        if (_file_deleted) {
            throw new FileAlreadyDeletedException(null, "file alread deleted");
        }

        // Mark the deleted flag (even if it doesn't get all the way done).
        _file_deleted = true;
        for (int column = 0; column < type.length; column++) {
            type[column].attrType = AttrType.attrNull;
        }
        type = null;
        for (int column=0;column<numColumns;column++){
                HF[column].deleteFile();   
        }
        Iterator <Map.Entry<String,BTreeFile>> iterator = BTMap.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<String,BTreeFile> entry= iterator.next();
            BTreeFile btf= entry.getValue();
            if(btf != null)
                btf.destroyFile();
        }
        numColumns = 0;

        Iterator <Map.Entry<String,BitMapFile>> iterator2 = BMMap.entrySet().iterator();
        while(iterator2.hasNext()){
            Map.Entry<String,BitMapFile> entry= iterator2.next();
            BitMapFile bmf= entry.getValue();
            if(bmf != null)
                bmf.destroyBitMapFile();
        }
    }

    public TID insertTuple(byte[] tuplePtr) throws FileAlreadyDeletedException,
            InvalidSlotNumberException, InvalidTupleSizeException,
            HFBufMgrException, HFDiskMgrException, IOException, HFException, SpaceNotAvailableException {
        TID tid = new TID(numColumns);
        short tuple_fldcount = Convert.getShortValue(0, tuplePtr);
        short[] tuple_fldoffsets = new short[tuple_fldcount];
        for (int column = 0; column < tuple_fldcount; column++) {
            tuple_fldoffsets[column] = Convert.getShortValue(2 + 2 * column, tuplePtr);
        }
        int pos = 0;
        for (int column = 0; column < type.length; column++) {
            byte[] temp = null;
            if (type[column].attrType == AttrType.attrInteger) {
                temp = new byte[4];
                System.arraycopy(tuplePtr, tuple_fldoffsets[column], temp, 0, 4);
                tid.recordIDs[column] = HF[column].insertRecord(temp);
            } else if (type[column].attrType == AttrType.attrString) {
                // if (column<tuple_fldcount){
                // System.arraycopy(tuplePtr, tuple_fldoffsets[column], temp, 0,
                // tuple_fldoffsets[column+1] - tuple_fldoffsets[column]);
                // }
                // else{
                // System.arraycopy(tuplePtr, tuple_fldoffsets[column], temp, 0,
                // tuple_offset+tuple_length - tuple_fldoffsets[column]);
                // }
                temp = new byte[strattrsize];
                System.arraycopy(tuplePtr, tuple_fldoffsets[column], temp, 0, strattrsize);
                tid.recordIDs[column] = HF[column].insertRecord(temp);
            } else if (type[column].attrType == AttrType.attrReal) {
                temp = new byte[4];
                System.arraycopy(tuplePtr, tuple_fldoffsets[column], temp, 0, 4);
                tid.recordIDs[column] = HF[column].insertRecord(temp);
            } else if (type[column].attrType == AttrType.attrSymbol) {
                temp = new byte[2];
                System.arraycopy(tuplePtr, tuple_fldoffsets[column], temp, 0, 2);
                tid.recordIDs[column] = HF[column].insertRecord(temp);
            } else {
                tid.recordIDs[column] = HF[column].insertRecord(temp);
            }
            
            // Update index files
            String btIndexname = getBTName(column);
            ValueClass val type[column].attrType == AttrType.attrString ? new StringValue(Convert.getStrValue(0, temp, strattrsize) : new IntegerValue(Convert.getIntValue(0, temp));
            String bmIndexname = getBMName(column, val);
            if (BTMap != null && BTMap.containsKey(btIndexname)) {
                pos = getColumn(column).positionOfRecord(tid.recordIDs[column]);
                KeyClass key = null;
                switch (type[column].attrType) {
                    case 0:
                    case 3:
                        String s = Convert.getStrValue(0, temp, strattrsize);
                        key = new StringKey(s);
                        break;
                    case 1:
                    case 2:
                        Integer i = Convert.getIntValue(0, temp);
                        key = new IntegerKey(i);
                        break;
                    default:
                        break;
                }
                getBTIndex(btIndexname).insert(key, pos);
            }
            if (BMMap != null && BMMap.containsKey(bmIndexname)) {
                pos = getColumn(column).positionOfRecord(tid.recordIDs[column]);
                getBMIndex(bmIndexname).insert(pos);
            }
            if(i+1 == numColumns){
                pos = getColumn(1).positionOfRecord(rids[1]);
            }
        }
        tid.position = pos;
        return tid;
        // need to figure out what to do with position variable- for BMmap and BT index
    }

    public Tuple getTuple(TID tid) throws IOException, FileAlreadyDeletedException,
            InvalidSlotNumberException, InvalidTupleSizeException, Exception,
            HFBufMgrException, HFDiskMgrException, IOException, HFException, SpaceNotAvailableException,
            FieldNumberOutOfBoundException {
        // Tuple tpl = new Tuple();
        // tpl.setHdr(0, type, null);
        byte[] arr = new byte[GlobalConst.MINIBASE_PAGESIZE];
        short offset = 0;
        int length = 0;
        short fldCnt = (short) numColumns;
        short[] fldOffset = new short[numColumns];

        for (int column = 1; column < numColumns + 1; column++) {
            Tuple temp_tuple = HF[column].getRecord(tid.recordIDs[column]);
            short[] o = temp_tuple.copyFldOffset();
            int l = temp_tuple.getLength();
            fldOffset[column - 1] = offset;
            // HashMap <int,int> attrmapping =
            // System.arraycopy(temp_tuple.data, o[0], arr,offset, l-o[0]);
            offset += l - o[0];
            if (type[column - 1].attrType == 0) {
                String s = temp_tuple.getStrFld(1);
                Convert.setStrValue(s, offset, arr);
                offset += s.length();
            } else if (type[column - 1].attrType == 1) {
                int temp = temp_tuple.getIntFld(1);
                Convert.setIntValue(temp, offset, arr);
                offset += 4;
            } else if (type[column - 1].attrType == 2) {
                float f = temp_tuple.getFloFld(1);
                Convert.setFloValue(f, offset, arr);
                offset += 4;
            } else if (type[column - 1].attrType == 3) {
                char c = temp_tuple.getCharFld(1);
                Convert.setCharValue(c, offset, arr);
                offset += 2;
            }
        }
        int tuple_length = offset;
        Tuple tuple = new Tuple(arr, 0, tuple_length);
        return tuple;
    }

    public ValueClass getValue(TID tid, int column) throws IOException, FileAlreadyDeletedException,
            InvalidSlotNumberException, InvalidTupleSizeException, Exception,
            HFBufMgrException, HFDiskMgrException, IOException, HFException, SpaceNotAvailableException,
            FieldNumberOutOfBoundException {
        Tuple temp_tuple = HF[column].getRecord(tid.recordIDs[column]);
        ValueClass val = null;
        if (type[column].attrType == 1) {
            val = new IntegerValue(temp_tuple.getIntFld(1));
        } else if (type[column].attrType == 2) {
            val = new IntegerValue((int) temp_tuple.getFloFld(1));
        } else if (type[column].attrType == 0) {
            val = new StringValue(temp_tuple.getStrFld(1));
        } else if (type[column].attrType == 3) {
            val = new StringValue(temp_tuple.getCharFld(1) + "");
        }
        return val;
    }

    public int getColumnNumberFromColName(String colName)
    {
        if(columnMap.containsKey(colName))
            return columnMap.get(colName);
        return -1;
    }

    public int getTupleCnt() throws IOException, FileAlreadyDeletedException,
            InvalidSlotNumberException, InvalidTupleSizeException, Exception,
            HFBufMgrException, HFDiskMgrException, IOException, HFException, SpaceNotAvailableException,
            FieldNumberOutOfBoundException {
        return HF[1].getRecCnt();

    }

    // --------implement this function--------------
    public TupleScan openTupleScan() throws Exception{
        
        TupleScan scanResult = new TupleScan(this);
        return scanResult;
    }

    public Scan openColumnScan(int columnNo) throws Exception {
        if (columnNo < numColumns)
            return HF[columnNo].openScan();
        else
            throw new Exception("Invalid Column number");
    }

    public boolean updateTuple(TID tid, Tuple newtuple) throws InvalidSlotNumberException, InvalidUpdateException,
            InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
        byte[] arr = null;
        int length = 0;
        boolean flag = true;
        for (int column = 0; column < numColumns; column++) {
            arr = null;
            length = 0;
            if (type[column].attrType == 1) {
                Convert.setIntValue(newtuple.getIntFld(column), 0, arr);
                length = 4;
            } else if (type[column].attrType == 2) {
                Convert.setFloValue(newtuple.getFloFld(column), 0, arr);
                length = 4;
            } else if (type[column].attrType == 3) {
                Convert.setCharValue(newtuple.getCharFld(column), 0, arr);
                length = 2;
            } else if (type[column].attrType == 0) {
                Convert.setStrValue(newtuple.getStrFld(column), 0, arr);
                length = newtuple.getStrFld(column).length();
            }
            Tuple tpl = new Tuple(arr, 0, length);
            flag = HF[column + 1].updateRecord(tid.recordIDs[column], tpl);
            if (!flag) {
                throw new Exception("Unable to perform Update");
            }
            // incase update fails dk how to rollback previous updates
        }
        return flag;
    }

    public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column)
            throws InvalidSlotNumberException, InvalidUpdateException, InvalidTupleSizeException, HFException,
            HFDiskMgrException, HFBufMgrException, Exception {
        byte[] arr = null;
        int length = 0;
        boolean flag = true;
        if (type[column].attrType == 1) {
            Convert.setIntValue(newtuple.getIntFld(column), 0, arr);
            length = 4;
        } else if (type[column].attrType == 2) {
            Convert.setFloValue(newtuple.getFloFld(column), 0, arr);
            length = 4;
        } else if (type[column].attrType == 3) {
            Convert.setCharValue(newtuple.getCharFld(column), 0, arr);
            length = 2;
        } else if (type[column].attrType == 0) {
            Convert.setStrValue(newtuple.getStrFld(column), 0, arr);
            length = newtuple.getStrFld(column).length();
        }
        Tuple tpl = new Tuple(arr, 0, length);
        flag = HF[column + 1].updateRecord(tid.recordIDs[column], tpl);
        if (!flag) {
            throw new Exception("Unable to perform Update");
        }
        return true;
    }

    public String generateBTName(int column) {
        return "BT." + CFname + "." + column;
    }

    public String getDeletedFileName() {
        return CFname + ".del";
    }

    public String generateBMName(int columnNo, ValueClass value) {
        return "BM" + "." + get_ColumnarFile_name() + "." + columnNo + "." + value.toString();
    }

    public boolean createBTreeIndex(int column) throws Exception {
        int keysize = 0;
        String indexName = generateBTName(column);
        keysize = get_attr_size(column);
        BTreeFile bTreeFile = new BTreeFile(indexName, type[column].attrType, keysize, DeleteFashion.FULL_DELETE);
        Scan colScan = openColumnScan(column);
        RID rid = new RID();
        Tuple tpl;
        KeyClass key = null;
        Object keyvalue;
        while (true) {
            tpl = colScan.getNext(rid);
            if (tpl == null)
                break;
            switch (type[column].attrType) {
                case 0:
                case 3:
                    key = new StringKey(getval(column, tpl).toString());
                    break;
                case 1:
                case 2:
                    key = new IntegerKey((int) getval(column, tpl));
                default:
                    break;
            }
            bTreeFile.insert(key, rid);
        }
        colScan.closescan();
        addIndexToColumnar(0, indexName);
        return true;
    }

    public boolean createBitMapIndex(int columnNo, ValueClass value) throws Exception {

        short[] targetedCols = new short[1];
        targetedCols[0] = (short) columnNo;

        FldSpec[] projection = new FldSpec[1];
        projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

        ColumnarColumnScan columnScan = new ColumnarColumnScan(get_ColumnarFile_name(), columnNo,
                projection,
                targetedCols,
                null, null);

        String indexName = generateBMName(columnNo, value);
        BitMapFile bitMapFile = new BitMapFile(indexName, this, columnNo, value);
        Tuple tuple;
        int position = 0;
        while (true) {
            tuple = columnScan.get_next();
            if (tuple == null) {
                break;
            }

            ValueClass v;
            switch (type[columnNo].attrType) {
                case 0:
                    v = new IntegerValue(tuple.getIntFld(1));
                    break;
                case 3:
                    v = new StringValue(tuple.getStrFld(1));
                    break;
                default:
                    v = new StringValue(tuple.getStrFld(1));
                    break;
            }
            if (v.toString().equals(value.toString())) {
                bitMapFile.Insert(position);
            } else {
                bitMapFile.Delete(position);
            }
            position++;
        }
        columnScan.close();
        bitMapFile.close();

        addIndexToColumnar(1, indexName);

        return true;
    }

    public int get_numcols() {
        return numColumns;
    }

    public Heapfile get_HF(int column) {
        return HF[column];
    }

    public AttrType[] get_AttrTypes() {
        return type;
    }

    public AttrType getAttrtypeforcolumn(int column) {
        return type[column];
    }

    public String get_ColumnarFile_name() {
        return CFname;
    }

    public short get_attr_size(int column) {
        switch (type[column].attrType) {
            case 0:
                return (short) strattrsize;
            case 1:
            case 2:
                return 4;
            case 3:
                return 2;
            default:
                return 0;
        }
    }

    public Object getval(int column, Tuple tpl) throws FieldNumberOutOfBoundException, IOException {
        switch (type[column].attrType) {
            case 0:
                return tpl.getStrFld(1);
            case 1:
                return tpl.getIntFld(1);
            case 2:
                return tpl.getFloFld(1);
            case 3:
                return tpl.getCharFld(1);
            default:
                return null;
        }
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
            AttrType[] itypes = new AttrType[2];
            itypes[0] = new AttrType(AttrType.attrInteger);
            itypes[1] = new AttrType(AttrType.attrString);
            short[] isizes = new short[1];
            isizes[0] = 40; // index name can't be more than 40 chars
            Tuple t = new Tuple();
            t.setHdr((short) 2, itypes, isizes);
            int size = t.size();
            t = new Tuple(size);
            t.setHdr((short) 2, itypes, isizes);
            t.setIntFld(1, indexType);
            t.setStrFld(2, indexName);
            Heapfile f = new Heapfile(CFname + ".idx");
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

    public String[] getAvailableBM(int columnNo) {
        List<String> bmName = new ArrayList<>();
        String prefix = "BM" + "." + CFname + "." + columnNo + ".";
        for (String s : BMMap.keySet()) {
            if (s.substring(0, prefix.length()).equals(prefix)) {
                bmName.add(s);
            }
        }
        return bmName.toArray(new String[bmName.size()]);
    }

}
