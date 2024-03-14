package columnar;

import global.AttrType;
import global.RID;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

import java.io.IOException;

public class TupleScan {
    private Columnarfile file;
	Scan[] scan;

	private AttrType[] attrTypes = null;
	private short[] actualSize = null;
	private short[] strSize = null;
	private short numColumns;
	private int toffset;
	private int tuplesize;

    /**
     * Contructor of TupleScan for all columns
     *
     * @param cf Columnarfile object
     * @throws InvalidTupleSizeException
     * @throws IOException
     */
	public TupleScan(Columnarfile cf) throws Exception {
        file = cf;
        numColumns = cf.numColumns;
        attrTypes = cf.attrTypes;
        actualSize = cf.actualSize;
        strSize = cf.getStrSize();
        toffset = cf.getOffset();
        tuplesize = cf.getTupleSize();
        scan = new Scan[numColumns];
        for(int i=0;i<numColumns;i++){
            scan[i] = cf.getColumn(i).openScan();
        }
    }

    /**
     * Constructor of TupleScan for specified columns
     *
     * @param cf Columnarfile object
     * @param columns array of column numbers that need to be scanned
     * @throws InvalidTupleSizeException
     * @throws IOException
     */
    public TupleScan(Columnarfile cf, short[] columns) throws Exception {
        file = cf;
        numColumns = (short)columns.length;
        attrTypes = new AttrType[numColumns];
        actualSize = new short[numColumns];
        scan = new Scan[numColumns];
        short strCnt = 0;
        for(int i=0;i<numColumns;i++){

            short c = columns[i];
            attrTypes[i] = cf.attrTypes[c];
            actualSize[i] = cf.actualSize[c];
            scan[i] = cf.getColumn(c).openScan();

            if(attrTypes[i].attrType == AttrType.attrString)
                strCnt++;
        }

        strSize = new short[strCnt];
        toffset = getOffset();
        tuplesize = toffset;
        int cnt = 0;
        for(int i = 0; i < numColumns; i++){
            short c = columns[i];
            if(attrTypes[i].attrType == AttrType.attrString) {
                strSize[cnt++] = cf.attrSize[c];
            }
            tuplesize += actualSize[i];
        }
    }
    
    /**
     * Close all files of tupleScan obj
     */
	public void closetuplescan(){
		for(int i=0;i<scan.length;i++){
			scan[i].closescan();
		}
        file.close();
	}
	
	/**
	 * Gets next Tuple in Scan
	 * @param tid
	 * @return next tuple
	 * @throws Exception
	 */
	public Tuple getNext(TID tid) throws Exception {

        Tuple result = new Tuple(tuplesize);
        result.setHdr(numColumns, attrTypes, strSize);
        byte[] data = result.getTupleByteArray();
        RID[] rids = new RID[scan.length];
        RID rid = new RID();
        int pos = 0;
        int offset = toffset;
        for (int i = 0; i < numColumns; i++) {
            Tuple t = scan[i].getNext(rid);
            if (t == null)
                return null;

            rids[i] = new RID();
            rids[i].copyRid(rid);
            rid = new RID();
            int size = actualSize[i];
            System.arraycopy(t.getTupleByteArray(), 0, data, offset, size);
            offset += actualSize[i];
            if(i+1 == numColumns)
                pos = scan[i].positionOfRecord(rids[i]);
        }

        tid.numRIDs = scan.length;
        tid.recordIDs = rids;
        tid.setPosition(pos);
        result.tupleInit(data, 0, data.length);

        return result;

    }
	
	/**
	 * Position all scan cursors to the records with the given rids
	 * @param tid
	 * @return true if success
	 */
	public boolean position(TID tid){
		RID[] rids = new RID[tid.numRIDs];
		for(int i=0;i<tid.numRIDs;i++){
			rids[i].copyRid(tid.recordIDs[i]);
			try {
				boolean ret = scan[i].position(rids[i]);
				if(ret == false){
					return false;
				}
			} catch (InvalidTupleSizeException e) {

				e.printStackTrace();
			} catch (IOException e) {

				e.printStackTrace();
			}
		}
		return true;
	}
	
	/**
	 * Gets offset
	 * @return offset
	 */
    public int getOffset() {
        return 2 * (numColumns + 2);
    }

}