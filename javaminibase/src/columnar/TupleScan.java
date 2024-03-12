package columnar;

import java.io.IOException;
import java.util.Arrays;

import global.*;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

public class TupleScan implements GlobalConst {
    private Columnarfile cf;
    private int ncols;
    AttrType[] attrTypes;
    short[] str_attr_sizes;
    short str_size = 25;
    Scan[] sc;
    int tuplesize;

    public TupleScan(Columnarfile f) throws InvalidTupleSizeException, IOException {
        cf = f;
        ncols = f.get_numcols();
        sc = new Scan[ncols];
        for (int i = 1; i < ncols + 1; i++) {
            sc[i - 1] = f.get_HF(i).openScan();
        }
        attrTypes = f.get_AttrTypes();
        int str_attr_count = 0;
        tuplesize = 2 * (ncols) + 2;
        for (int i = 0; i < ncols; i++) {
        if (attrTypes[i].attrType == AttrType.attrString) {
            str_attr_count++;
            tuplesize += str_size;
        }
        else
            tuplesize += 4;
        }
        str_attr_sizes = new short[str_attr_count];
        Arrays.fill(str_attr_sizes, str_size);
    }
        

    /**
    * This constructor truly takes advantage of columnar organization by scanning only
    * required columns.
    *
    * @param f Columnarfile object
    * @param columns array of column numbers that need to be scanned
    * @throws InvalidTupleSizeException
    * @throws IOException
    */
    public TupleScan(Columnarfile f,short[] columns) throws Exception {
        cf = f;
        ncols = (short)columns.length;
        attrTypes = new AttrType[ncols];
        sc=new Scan[ncols];
        short strCnt = 0;
        tuplesize = 2 * (ncols) + 2;
        for(int i=0;i<ncols;i++){
        
        
        short c = columns[i];
        attrTypes[i] = f.type[c];
        sc[i] = f.getColumn(c).openScan();
        
        
        if(attrTypes[i].attrType == AttrType.attrString){
        tuplesize += str_size;
        strCnt++;
        }
        else
        tuplesize += 4;
        }
        
        
        str_attr_sizes = new short[strCnt];
        Arrays.fill(str_attr_sizes, str_size);
    }
    

    public void closetuplescan() {
        for (int i = 0; i < ncols; i++) {
            sc[i].closescan();
        }
    }

    public Tuple getNext(TID tid) throws Exception {
        Tuple tpl = new Tuple(tuplesize);
        tpl.setHdr((short) ncols, attrTypes, str_attr_sizes);
        RID[] rids = new RID[ncols];
        byte[] data = tpl.getTupleByteArray();
        int offset = 2 * (ncols) + 2;;
        int position = 0;
        for (int i = 0; i < ncols; i++) {
            RID rid = new RID();
            Tuple temp = sc[i].getNext(rid);
            if (temp == null)
                return null;
            rids[i].copyRid(rid);
            System.arraycopy(temp.getTupleByteArray(), 0, data, offset, cf.get_attr_size(i));
            offset += cf.get_attr_size(i);
        }
        position = sc[0].positionOfRecord(rids[0]);
        tid.numRIDs = ncols;
        tid.recordIDs = rids;
        tid.setPosition(position);
        tpl.tupleInit(data, 0, offset);
        return tpl;
    }

    public boolean position(TID tid) throws InvalidTupleSizeException, IOException {
        for (int i = 0; i < tid.numRIDs; i++) {
            boolean flag = sc[i].position(tid.recordIDs[i]);
            if (!flag)
                return false;
        }
        return true;
    }

}
