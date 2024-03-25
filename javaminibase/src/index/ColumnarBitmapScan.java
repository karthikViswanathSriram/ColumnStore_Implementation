package index;

import bitmap.BMPage;
import bitmap.BitMapFile;
import bitmap.BitmapFileScan;
import btree.PinPageException;
import columnar.Columnarfile;
import diskmgr.Page;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class ColumnarBitmapScan extends Iterator implements GlobalConst{
    private CondExpr[] conditions;
    private boolean index_only;
    private int columnNo;

    private List<BitmapFileScan> bmScans;
    private Columnarfile cf;
    private BitSet bitMaps;
    private int counter = 0;
    private int scanCounter = 0;
    private int pageCounter = 0;

    public ColumnarBitmapScan(Columnarfile cf, int columnNo, CondExpr[] conditions, boolean indexOnly) throws IndexException {

        this.conditions = conditions;
        this.index_only = indexOnly;
        this.columnNo = columnNo;
        try {
            this.cf = cf;
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            bmScans = new ArrayList<>();
            for (String bmName : cf.getAvailableBM(columnNo)){
                if(evalBMName(bmName)){
                    bmScans.add((new BitMapFile(bmName)).new_scan());
                }
            }
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: BitMapFile exceptions caught from BitMapFile constructor");
        }
    }

    @Override
    public Tuple get_next() throws IndexException, UnknownKeyTypeException {
        return get_next_BM();
    }

    public boolean delete_next() throws IndexException, UnknownKeyTypeException {

        return delete_next_BM();
    }

    public Tuple get_next_BM(){
        int position = 0;
        while (position != -1) {
            try {
                position = get_next_position();
            	while (position == -2) {
            		position = get_next_position();
            	}
            	if (position == -1)
                    return null;
                Tuple t = new Tuple(10);
                AttrType[] type = new AttrType[1];
                type[0] = new AttrType(AttrType.attrInteger);
                short[] sizes = new short[0];
                t.setHdr((short)1, type, sizes);
                t.setIntFld(1, position);
                return t;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private boolean delete_next_BM() throws IndexException, UnknownKeyTypeException {
        int position = get_next_position();
        if(position < 0)
            return false;

        return cf.markTupleDeleted(position);
    }

    public int get_next_position(){
        try {

            if (scanCounter == 0) {
                bitMaps = new BitSet();
                for(BitmapFileScan s : bmScans){
                    counter = s.counter;
                    BitSet bs = s.get_next_bitmap();
                    if(bs == null) {
                        return -1;
                    }
                    else {
                        bitMaps.or(bs);
                    }
                }
            }
            
            while (scanCounter <= counter) {
                if (bitMaps.get(scanCounter)) {
                    int position = scanCounter + pageCounter;
                    scanCounter++;
                    return position;
                } else {
                    scanCounter++;
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        scanCounter = 0;
        pageCounter += counter;
        return -2;
    }

    public void close() throws Exception {
        if (!closeFlag) {
            closeFlag = true;
            for(BitmapFileScan s : bmScans){
                s.close();
            }
        }
    }

    boolean evalBMName(String s) throws Exception {
        if(this.conditions == null)
            return true;

        short[] attrSizes = new short[1];
        attrSizes[0] = cf.getAttrsizeforcolumn(columnNo);
        AttrType[] attrTypes = new AttrType[1];
        attrTypes[0] = cf.getAttrtypeforcolumn(columnNo);

        byte[] data = new byte[6+attrSizes[0]];
        String val = s.split("\\.")[3];
        if(attrTypes[0].attrType == AttrType.attrInteger) {
            int t = Integer.parseInt(val);
            Convert.setIntValue(t,6, data);
        }else {
            Convert.setStrValue(val, 6, data);
        }
        Tuple jTuple = new Tuple(data,0,data.length);
        attrSizes[0] -= 2;

        jTuple.setHdr((short)1,attrTypes, attrSizes);

        if(PredEval.Eval(this.conditions,jTuple,null,attrTypes, null))
            return true;

        return false;
    }


}
