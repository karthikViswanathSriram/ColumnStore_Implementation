package columnar;

import java.io.IOException;
import java.lang.*;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidUpdateException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import java.util.*;

import btree.BTreeFile;
public class Columnarfile {
    static int numColumns;
    AttrType[] type;
    private boolean _file_deleted;
    private Heapfile[] HF;
    private String CFname;

    public Columnarfile(java.lang.String name, int nColumns, AttrType[] type) {
        try {
            HF = new Heapfile[numColumns+1];
            Heapfile hdrFile = new Heapfile(name + ".hdr");
            HF[0] = (Heapfile)hdrFile;
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (int column = 1; column < numColumns+1; column++) {
            try {
                HF[column] = new Heapfile(name + ".Col" + column);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.CFname = name;
        this.type = type;
        this._file_deleted = false;
        numColumns = nColumns;
        //need to add content for header file
    }

    void deleteColumnarFile() throws FileAlreadyDeletedException,
                                InvalidSlotNumberException,InvalidTupleSizeException,
                                HFBufMgrException,HFDiskMgrException,IOException,HFException {
        if (_file_deleted) {
            throw new FileAlreadyDeletedException(null, "file alread deleted");
        }

        // Mark the deleted flag (even if it doesn't get all the way done).
        _file_deleted = true;
        numColumns = 0;
        for (int column = 0; column < type.length; column++) {
            type[column].attrType = AttrType.attrNull;
        }
        type = null;
        for (int column=0;column<numColumns;column++){
                HF[column].deleteFile();   
        }
    }

    TID insertTuple(byte[] tuplePtr) throws FileAlreadyDeletedException,
                                 InvalidSlotNumberException,InvalidTupleSizeException,
                                 HFBufMgrException,HFDiskMgrException,IOException,HFException,SpaceNotAvailableException{
        TID tid = new TID(numColumns);
        int tuple_offset = Convert.getIntValue(0, tuplePtr);
        int tuple_length = Convert.getIntValue(4, tuplePtr);
        short tuple_fldcount = Convert.getShortValue(8, tuplePtr);
        short[] tuple_fldoffsets = new short[tuple_fldcount];
        for (int column=0;column<tuple_fldcount;column++){
            tuple_fldoffsets[column] = Convert.getShortValue(10+2*column, tuplePtr);
        }
        for (int column=0;column<type.length;column++){
            byte[] temp = null;
            if (type[column].attrType == AttrType.attrInteger){
                System.arraycopy(tuplePtr, tuple_fldoffsets[column], temp, 0, 4); 
                tid.recordIDs[column] = HF[column].insertRecord(temp);
            }
            else if (type[column].attrType == AttrType.attrString){
                if (column<tuple_fldcount){
                    System.arraycopy(tuplePtr, tuple_fldoffsets[column], temp, 0, tuple_fldoffsets[column+1] - tuple_fldoffsets[column]); 
                }
                else{
                    System.arraycopy(tuplePtr, tuple_fldoffsets[column], temp, 0, tuple_offset+tuple_length - tuple_fldoffsets[column]);    
                }
                tid.recordIDs[column] = HF[column].insertRecord(temp);
            }
            else if (type[column].attrType == AttrType.attrReal){
                System.arraycopy(tuplePtr, tuple_fldoffsets[column], temp, 0, 4); 
                tid.recordIDs[column] = HF[column].insertRecord(temp);
            }
            else if (type[column].attrType == AttrType.attrSymbol){
                System.arraycopy(tuplePtr, tuple_fldoffsets[column], temp, 0, 2); 
                tid.recordIDs[column] = HF[column].insertRecord(temp);
            }
            else{
                tid.recordIDs[column] = HF[column].insertRecord(temp);
            }
        }
        return tid;
    //need to figure out what to do with position variable
    }

    Tuple getTuple(TID tid) throws IOException,FileAlreadyDeletedException,
                        InvalidSlotNumberException,InvalidTupleSizeException,Exception,
                        HFBufMgrException,HFDiskMgrException,IOException,HFException,SpaceNotAvailableException,FieldNumberOutOfBoundException{
        // Tuple tpl = new Tuple();
        // tpl.setHdr(0, type, null);
        byte[] arr = new byte[GlobalConst.MINIBASE_PAGESIZE];
        short offset = 0;
        int length = 0;
        short fldCnt = (short)numColumns;
        short[] fldOffset = new short[numColumns];

        for(int column=1;column<numColumns+1;column++){
            Tuple temp_tuple = HF[column].getRecord(tid.recordIDs[column]);
            short[] o = temp_tuple.copyFldOffset();
            int l = temp_tuple.getLength();
            fldOffset[column-1] = offset;
            // HashMap <int,int> attrmapping =  
            // System.arraycopy(temp_tuple.data, o[0], arr,offset, l-o[0]);
            offset+=l-o[0];
            if (type[column-1].attrType==0){
                String s = temp_tuple.getStrFld(0);
                Convert.setStrValue(s, offset, arr);
                offset+= s.length();
            }
            else if (type[column-1].attrType==1){
                int temp = temp_tuple.getIntFld(0);
                Convert.setIntValue(temp, offset, arr);
                offset+=4;
            }
            else if(type[column-1].attrType==2){
                float f = temp_tuple.getFloFld(0);
                Convert.setFloValue(f, offset, arr);
                offset+=4;
            }
            else if(type[column-1].attrType==3){
                char c = temp_tuple.getCharFld(0);
                Convert.setCharValue(c, offset, arr);
                offset+=2;
            }
        }
        int tuple_length = offset;
        Tuple tuple = new Tuple(arr,0,tuple_length);
        return tuple;
    }

    ValueClass getValue(TID tid, int column) throws IOException,FileAlreadyDeletedException,
    InvalidSlotNumberException,InvalidTupleSizeException,Exception,
    HFBufMgrException,HFDiskMgrException,IOException,HFException,SpaceNotAvailableException,FieldNumberOutOfBoundException{
        Tuple temp_tuple = HF[column].getRecord(tid.recordIDs[column]);
        ValueClass val = null;
        if (type[column].attrType==1){
            val = new IntegerValue(temp_tuple.getIntFld(0));
        } 
        else if (type[column].attrType==2){
            val = new IntegerValue((int) temp_tuple.getFloFld(0));
        }
        else if (type[column].attrType==0){
            val = new StringValue(temp_tuple.getStrFld(0));
        }
        else if (type[column].attrType==3){
            val = new StringValue(temp_tuple.getCharFld(0)+"");
        }
        return val;
    }

    int getTupleCnt()throws IOException,FileAlreadyDeletedException,
    InvalidSlotNumberException,InvalidTupleSizeException,Exception,
    HFBufMgrException,HFDiskMgrException,IOException,HFException,SpaceNotAvailableException,FieldNumberOutOfBoundException{
        return HF[1].getRecCnt();
        
    }

    //--------implement this function--------------
    TupleScan openTupleScan(){
        return null;
    }

    Scan openColumnScan(int columnNo) throws Exception{
        if(columnNo<numColumns)
            return HF[columnNo].openScan();
        else
            throw new Exception("Invalid Column number");
    }

    boolean updateTuple(TID tid, Tuple newtuple) throws InvalidSlotNumberException, InvalidUpdateException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception{
        byte[] arr=null;
        int length = 0;
        boolean flag = true;
        for(int column=0;column<numColumns;column++){
            arr = null;
            length = 0;
            if (type[column].attrType==1){
                Convert.setIntValue(newtuple.getIntFld(column), 0, arr);
                length = 4;
            }
            else if( type[column].attrType==2){
                Convert.setFloValue(newtuple.getFloFld(column), 0, arr);
                length = 4;
            }
            else if(type[column].attrType==3){
                Convert.setCharValue(newtuple.getCharFld(column), 0, arr);
                length = 2;
            }
            else if(type[column].attrType==0){
                Convert.setStrValue(newtuple.getStrFld(column), 0, arr);
                length = newtuple.getStrFld(column).length();
            }
            Tuple tpl = new Tuple(arr,0,length);
            flag = HF[column+1].updateRecord(tid.recordIDs[column], tpl);
            if (!flag){
                throw new Exception("Unable to perform Update");
            }
            //incase update fails dk how to rollback previous updates
        }
        return flag;
    }

    boolean updateColumnofTuple(TID tid, Tuple newtuple, int column) throws InvalidSlotNumberException, InvalidUpdateException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception{
        byte[] arr=null;
        int length = 0;
        boolean flag = true;
        if (type[column].attrType==1){
            Convert.setIntValue(newtuple.getIntFld(column), 0, arr);
            length = 4;
        }
        else if( type[column].attrType==2){
            Convert.setFloValue(newtuple.getFloFld(column), 0, arr);
            length = 4;
        }
        else if(type[column].attrType==3){
            Convert.setCharValue(newtuple.getCharFld(column), 0, arr);
            length = 2;
        }
        else if(type[column].attrType==0){
            Convert.setStrValue(newtuple.getStrFld(column), 0, arr);
            length = newtuple.getStrFld(column).length();
        }
        Tuple tpl = new Tuple(arr,0,length);
        flag = HF[column+1].updateRecord(tid.recordIDs[column], tpl);
        if (!flag){
            throw new Exception("Unable to perform Update");
        }
        return true;
    }

    boolean createBTreeIndex(int column) throws Exception{
        int ctype = type[column].attrType;
        int keysize = 0;
        Object key;
        String indexName = CFname + ".Column"+column+".BTFile";
        if (type[column].attrType==1){
            keysize = 4;
        }
        else if( type[column].attrType==2){
            keysize = 4;
        }
        else if(type[column].attrType==3){
            keysize = 2;
        }
        else if(type[column].attrType==0){
            Scan scan = openColumnScan(column);
            //need to complete
        }
        BTreeFile bTreeFile = new BTreeFile(indexName, type[column].attrType, keysize, 0);
        Scan colScan = openColumnScan(column);
        RID rid = new RID();
        Tuple tpl;
        while (true) {
            tpl = colScan.getNext(rid);
            if (tpl==null)
                break;
            
        }
        bTreeFile.insert(null, null);
        return true;
    }

    boolean createBitMapIndex(int columnNo, ValueClass value){
        

        return true;
    }
}
