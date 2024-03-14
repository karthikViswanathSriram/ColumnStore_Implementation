package interfaces;


import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import heap.Tuple;
import iterator.*;

public class DelQuery {

    public static void main(String args[]) throws Exception {
        /*
            Query Schema: COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE [NO PURGE]
        */
        String columnDB = args[0];
        String columnarFile = args[1];

        // Target Columns
        String[] targetColumns = args[2].split(",");
        String[] outputColumns = args[2].split(",");

        // Value Constraints
        String valueConstraints = args[3];

        // Num Buffers
        Integer numBuf = Integer.parseInt(args[4]);

        // ScanTypes
        String[] scanTypes = args[5].split(",");


        String purge = args[6];

        String dbpath = HelperFunctions.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbpath, 0, numBuf, "Clock");

        String[] temp2 = "".split(",");
        // Assumption that column name is only one character (To be changed later)
        String[] temp = new String[1];

        if(valueConstraints.length() == 0)
        {
            temp[0] = "";
        }
        else
        {
            temp[0] = valueConstraints.substring(0,1);
        }

        runDelQuery(columnarFile, outputColumns, valueConstraints, temp, scanTypes, temp2, targetColumns, purge);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        System.out.println("Reads: " + PCounter.rcounter);
        System.out.println("Writes: " + PCounter.wcounter);
    }

    private static void runDelQuery(String columnarFile, String[] outputColumns, String otherConstraints, String[] scanColumns, String[] scanTypes, String[] scanConstraints, String[] targetColumns, String purge) throws Exception {

        Columnarfile cf = new Columnarfile(columnarFile);

        AttrType[] opAttr = new AttrType[outputColumns.length];
        FldSpec[] outputColumnsList = new FldSpec[outputColumns.length];
        for (int i = 0; i < outputColumns.length; i++) 
        {
            String attribute = HelperFunctions.getAttributeName(outputColumns[i]);
            int colPos = HelperFunctions.getPositionTargetColumns(attribute, targetColumns);
            int attrPos = cf.getAttributePosition(attribute);
            int attrType = cf.getAttrtypeforcolumn(attrPos).attrType;

            outputColumnsList[i] = new FldSpec(new RelSpec(RelSpec.outer), colPos + 1);
            opAttr[i] = new AttrType(attrType);
        }

        int[] scanCols = new int[scanColumns.length];
        for (int i = 0; i < scanColumns.length; i++) {
            if (!scanColumns[i].equals("")) {
                String attribute = HelperFunctions.getAttributeName(scanColumns[i]);
                scanCols[i] = cf.getAttributePosition(attribute);
            }
        }

        short[] targets = new short[targetColumns.length];
        for (int i = 0; i < targetColumns.length; i++) {
            String attribute = HelperFunctions.getAttributeName(targetColumns[i]);
            targets[i] = (short) cf.getAttributePosition(attribute);
        }

        CondExpr[] otherConstraint = HelperFunctions.getCondExpr(otherConstraints, targetColumns);

        CondExpr[][] scanConstraint = new CondExpr[scanTypes.length][1];

        for (int i = 0; i < scanTypes.length; i++) {
            scanConstraint[i] = HelperFunctions.getCondExpr(scanConstraints[i]);
        }
        cf.close();
        Iterator it = null;
        int tupleCount = 0;
        try {
            if (scanTypes[0].equals("FILE")) 
            {
                ColumnarFileScan cfs;
                cfs = new ColumnarFileScan(columnarFile, outputColumnsList, targets, otherConstraint);
                Boolean deleted = true;
                while (deleted) 
                {
                    deleted = cfs.delete_next();
                    if (deleted == false) 
                    {
                        break;
                    }
                    tupleCount++;
                }
                cfs.close();
            } else if (scanTypes[0].equals("COLUMN")) 
            {
                ColumnarColumnScan ccs;
                ccs = new ColumnarColumnScan(columnarFile, scanCols[0], outputColumnsList, targets, scanConstraint[0], otherConstraint);
                Boolean deleted = true;
                while (deleted) 
                {
                    deleted = ccs.delete_next();
                    if (deleted == false) {
                        break;
                    }
                    tupleCount++;
                }
                ccs.close();

            } 
            else if (scanTypes[0].equals("BITMAP")) 
            {

                IndexType[] indexType = new IndexType[scanTypes.length];
                indexType[0] = new IndexType(IndexType.BitMapIndex);
               
                ColumnarIndexScan cis;
                cis = new ColumnarIndexScan(columnarFile, scanCols, indexType, scanConstraint, otherConstraint, false, targets, outputColumnsList, 0);
                Boolean deleted = true;
                while (deleted) 
                {
                    deleted = cis.delete_next(indexType);

                    if (deleted == false) {
                        break;
                    }
                    tupleCount++;
                }

                cis.close();

            }
            else if(scanTypes[0].equals("BTREE"))
            {
                IndexType[] indexType = new IndexType[scanTypes.length];
                indexType[0] = new IndexType(IndexType.B_Index);
                ColumnarIndexScan cis;
                cis = new ColumnarIndexScan(columnarFile, scanCols, indexType, scanConstraint, otherConstraint, false, targets, outputColumnsList, 0);
                Boolean deleted = true;
                while (deleted) 
                {
                    deleted = cis.delete_next(indexType);

                    if (deleted == false) {
                        break;
                    }
                    tupleCount++;
                }

                cis.close();
            } 
            else
            {
                throw new Exception("Scan type (" + scanTypes[0] + ") doesn't exist");
            }


            if(purge.equals("PURGE"))
            {
                cf.purgeAllDeletedTuples();
            }
            cf.close();

            System.out.println(tupleCount + " tuples were chosen");
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //it.close();
        }
    }
}
