package bitmap;

import java.util.ArrayList;
import java.util.List;

import btree.AddFileEntryException;
import btree.DeleteFileEntryException;
import btree.FreePageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import btree.UnpinPageException;
import columnar.Columnarfile;
import columnar.IntegerValue;
import columnar.StringValue;
import columnar.ValueClass;
import diskmgr.Page;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;
import heap.HFBufMgrException;

public class BitMapFile implements GlobalConst {

    private BitMapHeaderPage bmHeaderPage;
    private PageId bmHeaderPageId;
    private String columnarFileName;
    private Integer columnNumber;
    private AttrType attrType;
    private ValueClass value;

    /***
     * Getter: columnNumber
     * 
     * @return
     */
    public Integer getColumnNumber() {
        return columnNumber;
    }

    /***
     * Setter: columnNumber
     * 
     * @param columnNumber
     */
    public void setColumnNumber(Integer columnNumber) {
        this.columnNumber = columnNumber;
    }

    /***
     * Getter: value
     * 
     * @return
     */
    public ValueClass getValue() {
        return value;
    }

    /***
     * Setter: value
     * 
     * @param value
     */
    public void setValue(ValueClass value) {
        this.value = value;
    }

    /***
     * Getter: columnarFileName
     * 
     * @return
     */
    public String getColumnarFileName() {
        return columnarFileName;
    }

    /***
     * Setter: columnarFileName
     * 
     * @param fileName
     */
    public void setColumnarFileName(String fileName) {
        this.columnarFileName = fileName;
    }

    /***
     * Getter: bmHeaderPage
     * 
     * @return bmHeaderPage
     */
    public BitMapHeaderPage getHeaderPage() {
        return bmHeaderPage;
    }

    /***
     * Setter: bmHeaderPage
     * 
     * @param bmHeaderPage
     */
    public void setHeaderPage(BitMapHeaderPage bmHeaderPage) {
        this.bmHeaderPage = bmHeaderPage;
    }

    /***
     * Getter: bmHeaderPageId
     * 
     * @return bmHeaderPageId
     */
    public PageId getHeaderPageId() {
        return bmHeaderPageId;
    }

    public BitMapFile(String filename) throws Exception {
        // Constructor for opening an existing BitMapFile with given filename

        bmHeaderPageId = get_file_entry(filename);
        if (bmHeaderPageId == null) {
            throw new Exception("File does not exist");
        }
        bmHeaderPage = new BitMapHeaderPage(bmHeaderPageId);
        columnarFileName = bmHeaderPage.getColumnarFileName();
        columnNumber = bmHeaderPage.getColumnNumber();
        attrType = bmHeaderPage.getAttrType();

        /*
         * Check on lalit's part and change this. And where are we converting string to
         * integer
         */
        if (attrType.attrType == AttrType.attrString) {
            value = new StringValue(bmHeaderPage.getValue());
        } else {
            value = new IntegerValue(Integer.parseInt(bmHeaderPage.getValue()));
        }

    }

    public BitMapFile(String filename, Columnarfile columnarFile, Integer columnNo, ValueClass value)
            throws Exception {
        // Constructor for creating a new BitMapFile with given filename
        bmHeaderPageId = get_file_entry(filename);
        if (bmHeaderPageId != null) {
            bmHeaderPage = new BitMapHeaderPage(bmHeaderPageId);
            return;
        }
        bmHeaderPage = new BitMapHeaderPage();
        bmHeaderPageId = bmHeaderPage.getPageId();
        add_file_entry(filename, bmHeaderPageId);
        bmHeaderPage.set_rootId(new PageId(INVALID_PAGE));
        bmHeaderPage.setColumnarFileName(columnarFile.get_ColumnarFile_name());
        bmHeaderPage.setColumnNumber(columnNo);
        bmHeaderPage.setAttrType(attrType);

        if (value instanceof IntegerValue) {
            bmHeaderPage.setValue(((IntegerValue) value).getValue().toString());
        } else {
            bmHeaderPage.setValue(((StringValue) value).getValue());
        }

    }

    public boolean Insert(int position) throws Exception {
        setBitAtPosition(false, position);
        return true;
    }

    public boolean Delete(int position) throws Exception {
        setBitAtPosition(true, position);
        return true;
    }

    public void destroyBitMapFile() throws Exception, DeleteFileEntryException {
        if (bmHeaderPage != null) {
            PageId pgId = bmHeaderPage.get_rootId();
            BMPage bmPage = new BMPage();
            while (pgId.pid != INVALID_PAGE) {
                try {
                    Page page = pinPage(pgId);
                    bmPage.openBMpage(page);
                    pgId = bmPage.getNextPage();
                    unpinPage(pgId);
                    freePage(pgId);
                } catch (PinPageException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Error pinning page: " + e.getMessage());
                }
            }
            unpinPage(bmHeaderPageId);
            freePage(bmHeaderPageId);
            /* Delete the file entry */
            try {
                SystemDefs.JavabaseDB.delete_file_entry(columnarFileName);
            } catch (Exception e) {
                e.printStackTrace();
                throw new DeleteFileEntryException(e, "");
            }

            bmHeaderPage = null;
        }
    }

    public void close() throws Exception {
        // Close the BitMapFile
        if (bmHeaderPage != null) {
            SystemDefs.JavabaseBM.unpinPage(bmHeaderPageId, true);
            bmHeaderPage = null;
        }
    }

    /***
     * Update the bitmap value at the position specified.
     * 
     * @param set
     * @param position
     * @throws Exception
     */
    private void setBitAtPosition(boolean set, int position) throws Exception {
        List<PageId> pagesPinned = new ArrayList<>();
        if (bmHeaderPage == null) {
            throw new Exception("Bitmap header page is null");
        }
        if (bmHeaderPage.get_rootId().pid != INVALID_PAGE) {
            int pc;
            // Remove these comments
            // int pageCounter = 1;
            // pc is pageCounter
            for (pc = 1; position >= BMPage.NUM_POSITIONS_AVAILABLE_IN_PAGE; pc++) {
                // pageCounter++;
                position -= BMPage.NUM_POSITIONS_AVAILABLE_IN_PAGE;
            }
            PageId bmPageId = bmHeaderPage.get_rootId();
            Page page = pinPage(bmPageId);
            pagesPinned.add(bmPageId);
            BMPage bmPage = new BMPage(page);
            int i = 1;
            while (i < pc) {
                bmPageId = bmPage.getNextPage();
                if (bmPageId.pid == BMPage.INVALID_PAGE) {
                    PageId newPageId = getNewBMPage(bmPage.getCurPage());
                    pagesPinned.add(newPageId);
                    bmPage.setNextPage(newPageId);
                    bmPageId = newPageId;
                }
                page = pinPage(bmPageId);
                bmPage = new BMPage(page);
                i++;
            }
            byte[] data = bmPage.getBMpageArray();
            int bitPosition = position % 8;
            int bytePosition = position / 8;

            if (set == true) {
                data[bytePosition] |= (1 << bitPosition);
            } else {
                data[bytePosition] &= ~(1 << bitPosition);
            }
            bmPage.writeBMPageArray(data);
            int nValues = position + 1;

            if (bmPage.getCounter() < nValues) {
                bmPage.updateCounter((short) (nValues));
            }
        } else {
            PageId newPageId = getNewBMPage(bmHeaderPageId);
            pagesPinned.add(newPageId);
            bmHeaderPage.set_rootId(newPageId);
            setBitAtPosition(set, position);
        }
        for (PageId pagePinned : pagesPinned) {
            unpinPage(pagePinned, true);
        }
    }

    private PageId getNewBMPage(PageId prevPageId) throws Exception {
        Page apage = new Page();
        PageId pageId = newPage(apage, 1);
        BMPage bmPage = new BMPage();
        bmPage.init(pageId, apage);
        bmPage.setPrevPage(prevPageId);

        return pageId;
    }

    private PageId get_file_entry(String filename)
            throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

    private void add_file_entry(String fileName, PageId pageno)
            throws AddFileEntryException {
        try {
            SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AddFileEntryException(e, "");
        }
    }

    private Page pinPage(PageId pageno)
            throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }

    private void unpinPage(PageId pageno)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    private void unpinPage(PageId pageno, boolean dirty)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    private void freePage(PageId pageno)
            throws FreePageException {
        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new FreePageException(e, "");
        }

    }

    private PageId newPage(Page page, int num)
            throws HFBufMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseBM.newPage(page, num);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: newPage() failed");
        }

        return tmpId;

    }

}
