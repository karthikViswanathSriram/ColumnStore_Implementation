package iterator;

import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import global.*;
import heap.*;

import java.io.IOException;

public class ColumnarColumnScan extends Iterator{

    private Columnarfile cf;
    private Scan scan;
    private CondExpr[] OutputFilter;//conditional expression to evaluate the expression
    private CondExpr[] OtherFilter;
    public FldSpec[] perm_mat;
    private AttrType[] attrTypes = null;//get the attribute type of the attribute
    private short[] s_sizes = null;//the size of each attribute
    private Heapfile[] targetHF = null;
    private AttrType[] targetAttrTypes = null;
    private short[] targetSizes = null;
    private short[] targetCols = null;
    private short tupleSize;
    private Tuple    t;
    private int attrSize;
    int columnNo;
    Sort deletedTuples;
    private int currDeletePos = -1;
	
    public ColumnarColumnScan(String file_name, int columnNo, FldSpec[] proj_list, short[] targetedCols, CondExpr[] outFilter, CondExpr[] otherFilter) throws FileScanException, TupleUtilsException, IOException, InvalidRelation {
        this.targetCols = targetedCols;
        this.OutputFilter = outFilter;
        this.OtherFilter = otherFilter;
        this.perm_mat = proj_list;
        this.columnNo = columnNo;
        try {
            this.cf = new Columnarfile(file_name);
            this.targetHF = ColumnarScanUtils.getTargetHeapFiles(cf, targetedCols);
            this.targetAttrTypes = ColumnarScanUtils.getTargetColumnAttributeTypes(cf, targetedCols);
            this.targetSizes = ColumnarScanUtils.getTargetColumnStringSizes(cf, targetedCols);
            t = ColumnarScanUtils.getProjectionTuple(cf, perm_mat, targetedCols);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            scan = cf.openColumnScan(columnNo);
            attrTypes = new AttrType[1];
            attrTypes[0] = cf.getAttrtypeforcolumn(columnNo);
            s_sizes = new	short[1];
            s_sizes[0] = cf.getAttrsizeforcolumn(columnNo);
            attrSize = cf.getAttrsizeforcolumn(columnNo);
            Tuple t = new Tuple();
            t.setHdr((short)1, attrTypes, s_sizes);
            tupleSize = t.size();
            PageId pid = SystemDefs.JavabaseDB.get_file_entry(cf.getDeletedFileName());
            if (pid != null) {
                FldSpec[] projlist = new FldSpec[1];
                projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
                FileScan fs = new FileScan(cf.getDeletedFileName(), attrTypes, s_sizes, (short)1, 1, projlist, null);
                deletedTuples = new Sort(attrTypes, (short) 1, s_sizes, fs, 1, new TupleOrder(TupleOrder.Ascending), 4, 10);
            }
        } catch (Exception e) {
            throw new FileScanException(e, "openScan() failed");
        }

    }

    public FldSpec[] show() {
        return perm_mat;
    }

    /**
     * @return the result tuple
     * @throws JoinsException                 some join exception
     * @throws IOException                    I/O errors
     * @throws InvalidTupleSizeException      invalid tuple size
     * @throws InvalidTypeException           tuple type not valid
     * @throws PageNotReadException           exception from lower layer
     * @throws PredEvalException              exception from PredEval class
     * @throws UnknowAttrType                 attribute type unknown
     * @throws FieldNumberOutOfBoundException array out of bounds
     * @throws WrongPermat                    exception for wrong FldSpec argument
     */
    public Tuple get_next()
            throws Exception {

        while (true) {
            int position = getNextPosition();
            if (position < 0) return null;

            Tuple tTuple = null;
            try {
                tTuple = new Tuple();
                tTuple.setHdr((short) this.targetCols.length, targetAttrTypes, targetSizes);
                tTuple = new Tuple(tTuple.size());
                tTuple.setHdr((short) this.targetCols.length, targetAttrTypes, targetSizes);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }

            for (int i = 0; i < targetHF.length; i++) {
                Tuple targetRecord = targetHF[i].getRecord(position);
                if (targetAttrTypes[i].attrType == AttrType.attrInteger)
                	tTuple.setIntFld(i + 1, Convert.getIntValue(0, targetRecord.getTupleByteArray()));
                else if (targetAttrTypes[i].attrType == AttrType.attrString)
                	tTuple.setStrFld(i + 1, Convert.getStrValue(0, targetRecord.getTupleByteArray(), targetSizes[i] + 2));
                else
                    throw new Exception("Attribute indexAttrType not supported");
            }

            if (PredEval.Eval(OtherFilter, tTuple, null, targetAttrTypes, null) == true) {
                Projection.Project(tTuple, targetAttrTypes, t, perm_mat, perm_mat.length);
                return t;
            }
        }
    }

    public boolean delete_next()
            throws Exception{

        int position = getNextPosition();
        if (position < 0)
            return false;
        return cf.markTupleDeleted(position);
    }

    private int getNextPosition()throws Exception {
        RID rid = new RID();

        Tuple t = null;
        while (true) {
            if ((t = scan.getNext(rid)) == null) {
                return -1;
            }

            Tuple tuple1= new Tuple(tupleSize);
            tuple1.setHdr((short)1, attrTypes, s_sizes);
            byte[] data = tuple1.getTupleByteArray();
            System.arraycopy(t.getTupleByteArray(), 0, data, 6, attrSize);
            t.tupleInit(data, 0, data.length);
            t.setHeaderMetaData();
            if (PredEval.Eval(OutputFilter, t, null, attrTypes, null) == true) {
                int position = cf.getColumn(columnNo).positionOfRecord(rid);
                if(deletedTuples != null && position > currDeletePos){
                    while (true){
                        Tuple dtuple = deletedTuples.get_next();
                        if(dtuple == null)
                            break;
                        currDeletePos = dtuple.getIntFld(1);
                        if(currDeletePos >= position)
                            break;
                    }
                }
                if(position == currDeletePos){
                    Tuple dtuple = deletedTuples.get_next();
                    if(dtuple == null)
                        break;
                    currDeletePos = dtuple.getIntFld(1);
                    continue;
                }
                return position;
            }
        }
        return -1;
    }

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     */
    public void close() throws IOException, SortException {

        if (!closeFlag) {
            scan.closescan();
            if(deletedTuples != null)
                deletedTuples.close();
            closeFlag = true;
        }
    }


}
