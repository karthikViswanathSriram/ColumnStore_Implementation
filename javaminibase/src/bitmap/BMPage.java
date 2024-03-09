package bitmap;

import java.io.*;
import global.*;
import diskmgr.*;

/**
 * Class heap file page.
 * The design assumes that records are kept compacted when
 * deletions are performed.
 */

@SuppressWarnings("unused")
public class BMPage extends Page implements GlobalConst {

    // This is the size of the page header of a BMPage (in bytes)
    public static final int DPHDR = 2 * 2 + 3 * 4;
    // This is the total number of bits available in a BMPage
    public static final int NUM_POSITIONS_AVAILABLE_IN_PAGE = (MAX_SPACE - DPHDR) * 8;

    // Removed TYPE, SLOT_CNT, USED_PTR from page metadata as they are used in heap
    // pages only.
    public static final int COUNTER = 0;
    public static final int FREE_SPACE = 2;
    public static final int PREV_PAGE = 4;
    public static final int NEXT_PAGE = 8;
    public static final int CUR_PAGE = 12;

    /*
     * Warning:
     * These items must all pack tight, (no padding) for
     * the current implementation to work properly.
     * Be careful when modifying this class.
     */

    /**
     * number of bits used
     */
    private short counter;

    /**
     * number of bits free in data[]
     */
    private short freeSpace;

    /**
     * backward pointer to data page
     */
    private PageId prevPage = new PageId();

    /**
     * forward pointer to data page
     */
    private PageId nextPage = new PageId();

    /**
     * page number of this page
     */
    protected PageId curPage = new PageId();

    /**
     * Default constructor
     */

    public BMPage() {

    }

    /**
     * Constructor of class BMPage
     * open a BMPage and make this BMPage point to the given page
     * 
     * @param page the given page in Page type
     */

    public BMPage(Page page) {
        data = page.getpage();
    }

    /**
     * Constructor of class HFPage
     * open a existed hfpage
     * 
     * @param apage a page in buffer pool
     */

    public void openBMpage(Page apage) {
        data = apage.getpage();
    }

    /**
     * Initialize a new page
     * 
     * @param pageNo the page number of a new page to be initialized
     * @param apage  the Page to be initialized
     * @see Page
     * @exception IOException I/O errors
     */

    public void init(PageId pageNo, Page apage)
            throws IOException {
        data = apage.getpage();

        counter = (short) 0;
        Convert.setShortValue(counter, COUNTER, data);

        freeSpace = (short) NUM_POSITIONS_AVAILABLE_IN_PAGE; // amount of space available
        Convert.setShortValue(freeSpace, FREE_SPACE, data);

        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);

        nextPage.pid = prevPage.pid = INVALID_PAGE;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);

        // Setting empty bytes to 0
        for (Integer i = DPHDR; i < MAX_SPACE; i++) {
            Convert.setByteValue((byte) 0, i, data);
        }

    }

    /**
     * Dump contents of a page
     * 
     * @exception IOException I/O errors
     */
    public void dumpPage()
            throws IOException {
        int i;

        counter = Convert.getShortValue(COUNTER, data);
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);

        System.out.println("dumpPage");
        System.out.println("curPage= " + curPage.pid);
        System.out.println("nextPage= " + nextPage.pid);
        System.out.println("freeSpace= " + freeSpace);
        System.out.println("byteCnt= " + counter);

        for (i = DPHDR; i < DPHDR + counter; i++) {
            Byte val = Convert.getByteValue(i, data);
            System.out.print("Position: " + i + "Value: " + val);

        }

    }

    /**
     * @return PageId of previous page
     * @exception IOException I/O errors
     */
    public PageId getPrevPage()
            throws IOException {
        prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
        return prevPage;
    }

    /**
     * sets value of prevPage to pageNo
     * 
     * @param pageNo page number for previous page
     * @exception IOException I/O errors
     */
    public void setPrevPage(PageId pageNo)
            throws IOException {
        prevPage.pid = pageNo.pid;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
    }

    /**
     * @return page number of next page
     * @exception IOException I/O errors
     */
    public PageId getNextPage()
            throws IOException {
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        return nextPage;
    }

    /**
     * sets value of nextPage to pageNo
     * 
     * @param pageNo page number for next page
     * @exception IOException I/O errors
     */
    public void setNextPage(PageId pageNo)
            throws IOException {
        nextPage.pid = pageNo.pid;
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
    }

    /**
     * @return page number of current page
     * @exception IOException I/O errors
     */
    public PageId getCurPage()
            throws IOException {
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        return curPage;
    }

    /**
     * sets value of curPage to pageNo
     * 
     * @param pageNo page number for current page
     * @exception IOException I/O errors
     */
    public void setCurPage(PageId pageNo)
            throws IOException {
        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);
    }

    /**
     * @return counter used in this page
     * @exception IOException I/O errors
     */
    public short getcounter()
            throws IOException {
        counter = Convert.getShortValue(COUNTER, data);
        return counter;
    }

    /**
     * sets value of counter to byteCtr
     * 
     * @param byteCtr number of bytes used in this page
     * @exception IOException I/O errors
     */
    public void setcounter(short byteCtr)
            throws IOException {
        counter = byteCtr;
        Convert.setShortValue(counter, COUNTER, data);
    }

    /**
     * returns the amount of available space on the page.
     * 
     * @return the amount of available space on the page
     * @exception IOException I/O errors
     */
    public int available_space() throws IOException {
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        return freeSpace;
    }

    /**
     * Determining if the page is empty
     * 
     * @return true if the BMPage is has no data in it, false otherwise
     * @exception IOException I/O errors
     */
    public boolean empty() throws IOException {

        int free_space = available_space();
        if (free_space == (MAX_SPACE - DPHDR) * 8) {
            return true;
        }
        return false;
    }

    public void updateCounter(Short value) throws IOException {
        Convert.setShortValue(value, COUNTER, data);
        Convert.setShortValue((short) (NUM_POSITIONS_AVAILABLE_IN_PAGE - value), FREE_SPACE, data);
    }

    /**
     * Returns the byte array of the BMPage
     * 
     * @return byte array
     * @throws IOException
     */

    public byte[] getBMpageArray() throws Exception {
        int dataLength = NUM_POSITIONS_AVAILABLE_IN_PAGE / 8;
        byte[] bitMapPageData = new byte[dataLength];
        for (int i = 0; i < dataLength; i++) {
            bitMapPageData[i] = Convert.getByteValue(DPHDR + i, data);
        }
        return bitMapPageData;
    }

    /**
     * Compacts the data in a BMPage
     * 
     * @exception IOException I/O errors
     */
    public void writeBMPageArray(Byte[] byteArray) throws IOException {

        for (int i = 0; i < byteArray.length; i++) {
            Convert.setByteValue(byteArray[i], DPHDR + i, data);
        }

    }

}
