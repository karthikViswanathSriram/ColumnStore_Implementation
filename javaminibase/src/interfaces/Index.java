package interfaces;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.SystemDefs;

import static global.GlobalConst.NUMBUF;

public class Index {

    public static void main(String[] args) throws Exception 
    {
        /*
            Query Schema: COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE
        */
        String columnDB = args[0];
        String columnarFileName = args[1];
        String columnName = args[2];
        String indexType = args[3];
        String dbPath = HelperFunctions.dbPath(columnDB);
        SystemDefs sysDef = new SystemDefs(dbPath, 0, NUMBUF, null);

        runIndex(columnarFileName, columnName, indexType);

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();

        // Add the function to display the read and write count
        System.out.println("Read Count: " + PCounter.rcounter);
        System.out.println("Write Count: " + PCounter.wcounter);
    }

    private static void runIndex(String columnarFile, String columnName, String indexType) throws Exception 
    {

        Columnarfile cf = new Columnarfile(columnarFile);
        int colNo = cf.getAttributePosition(columnName);

        if(indexType.equals("BTREE")) 
        {
            cf.createBTreeIndex(colNo);
        } 
        else 
        {
            cf.createAllBitMapIndexForColumn(colNo);
        }
        cf.close();

        System.out.println(indexType + " index was created successfully on "+ columnarFile + " file on column " + columnName);
    }
}
