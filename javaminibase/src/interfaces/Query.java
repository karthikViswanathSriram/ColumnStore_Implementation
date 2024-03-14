package interfaces;


import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.IndexType;
import global.SystemDefs;
import heap.Tuple;
import iterator.ColumnarIndexScan;
import iterator.*;

public class Query {

    public static void main(String args[]) throws Exception 
    {
        /*
            Query Schema: COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE
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

        String dbPath = HelperFunctions.dbPath(columnDB);
        SystemDefs sysdef = new SystemDefs(dbPath, 0, numBuf, null);



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
        runQuery(columnarFile, outputColumns, valueConstraints, temp, scanTypes, temp2, targetColumns);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        // Add the function to display the read and write count
        System.out.println("Read Count: " + PCounter.rcounter);
        System.out.println("Write Count: " + PCounter.wcounter);
    }

    private static void runQuery(String columnarFile, String[] outputColumns, String valueConstraints, String[] scanColumns, String[] scanTypes, String[] scanConstraints, String[] targetColumns) throws Exception {

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

        // 
        int[] scanCols = new int[scanColumns.length];
        for (int i = 0; i < scanColumns.length; i++) {
            if (!scanColumns[i].equals("")) {
                String attribute = HelperFunctions.getAttributeName(scanColumns[i]);
                scanCols[i] = cf.getAttributePosition(attribute);
            }
        }

        short[] targets = new short[targetColumns.length];
        for (int i = 0; i < targetColumns.length; i++)
        {
            String attribute = HelperFunctions.getAttributeName(targetColumns[i]);
            targets[i] = (short) cf.getAttributePosition(attribute);
        }
        cf.close();

        CondExpr[] valueConstraint = HelperFunctions.getCondExpr(valueConstraints, targetColumns);

        // Empty
        CondExpr[][] scanConstraint = new CondExpr[scanTypes.length][1];
        scanConstraint[0] = new CondExpr[1];
        scanConstraint[0] = null;
        
        Iterator it = null;
        try {
            if (scanTypes[0].equals("FILE")) 
            {
                it = new ColumnarFileScan(columnarFile, outputColumnsList, targets, valueConstraint);
            } else if (scanTypes[0].equals("COLUMN")) 
            {
                it = new ColumnarColumnScan(columnarFile, scanCols[0], outputColumnsList, targets, scanConstraint[0], valueConstraint);
            } 
            else if (scanTypes[0].equals("BITMAP")) 
            {

                IndexType[] indexType = new IndexType[scanTypes.length];
                indexType[0] = new IndexType(IndexType.BitMapIndex);
                it = new ColumnarIndexScan(columnarFile, scanCols, indexType, scanConstraint, valueConstraint, false, targets, outputColumnsList, 0);
            }
            else if(scanTypes[0].equals("BTREE")) 
            {
                IndexType[] indexType = new IndexType[scanTypes.length];
                indexType[0] = new IndexType(IndexType.B_Index);
                it = new ColumnarIndexScan(columnarFile, scanCols, indexType, scanConstraint, valueConstraint, false, targets, outputColumnsList, 0);
            }
            else
            {
                throw new Exception("Scan type (" + scanTypes[0] + ") doesn't exist");
            }

            int tupleCnt = 0;
            while (true) 
            {
                Tuple result = it.get_next();
                if (result == null) 
                {
                    break;
                }
                tupleCnt++;
                result.print(opAttr);
            }

            System.out.println(tupleCnt + " tuples were chosen");
            System.out.println();
        } 
        catch (Exception e) 
        {
            e.printStackTrace();
        } 
        finally 
        {
            it.close();
        }
    }
}
