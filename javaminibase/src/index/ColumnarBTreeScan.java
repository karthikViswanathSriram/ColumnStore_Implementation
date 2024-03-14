package index;

import btree.*;
import columnar.Columnarfile;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;

public class ColumnarBTreeScan extends Iterator implements GlobalConst{

    private String btName;
    private Columnarfile cf;
    private BTreeFile btFile;
    private IndexFileScan btScan;
    private CondExpr[] conditions;
    private boolean index_only;

    public ColumnarBTreeScan (Columnarfile cf, int columnNo, CondExpr[] selects, boolean indexOnly) throws IndexException {
        this.conditions = selects;
        this.index_only = indexOnly;
        try {

            this.cf = cf;
            this.btName = cf.getBTName(columnNo);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            this.btFile = new BTreeFile(this.btName);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
        }

        try {
            this.btScan = IndexUtils.BTree_scan(this.conditions, this.btFile);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
        }
    }

    @Override
    public Tuple get_next() throws IndexException, UnknownKeyTypeException {
        return get_next_BT();
    }

    public boolean delete_next() throws IndexException, UnknownKeyTypeException {

        return delete_next_BT();
    }

    private Tuple get_next_BT() throws IndexException, UnknownKeyTypeException {

        int position = 0;
        while (position != -1) {
            try {

                position = get_next_position();
                if (position < 0)
                    return null;
                // tuple that needs to sent
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

    private boolean delete_next_BT() throws IndexException, UnknownKeyTypeException {
        int position = -1;
        try {
            position = get_next_position();
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: BTree error");
        }

        if (position < 0)
            return false;

        return cf.markTupleDeleted(position);
    }

    public int get_next_position() throws IndexException, UnknownKeyTypeException {
        KeyDataEntry nextentry;
        try {
            nextentry = btScan.get_next();
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: BTree error");
        }

        if (nextentry == null)
            return -1;

        try {
            int position = ((LeafData) nextentry.data).getData();
            return position;
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: getRecord failed");
        }
    }


    public void close() throws IOException, JoinsException, SortException, IndexException, HFBufMgrException {
        if (!closeFlag) {
            closeFlag = true;
            try {
                btFile.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            btScan = null;
        }
    }

}
