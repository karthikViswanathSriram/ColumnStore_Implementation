package bitmap;

import global.*;
import diskmgr.Page;
import java.util.BitSet;
import java.io.IOException;
import index.IndexException;
import heap.HFBufMgrException;
import btree.PinPageException;
import iterator.SortException;
import iterator.JoinsException;


import static global.GlobalConst.INVALID_PAGE;

public class BitmapFileScan 
{

    BitMapFile file;
    private BitSet bitMaps;
    private BMPage currentBMPage;
    private int scanCounter = 0;
    private PageId currentPageId;
    public int counter;

   
    public BitmapFileScan(BitMapFile f) throws Exception 
    {
        file = f;
        currentPageId = file.getHeaderPage().get_rootId();

        // Pin current page
        Page pCurrentPage = pinPage(currentPageId);
        currentBMPage = new BMPage(pCurrentPage);

        // Number of entries (records) in the current page
        counter = currentBMPage.getCounter();

        // data bytes from the current page
        byte[] data = currentBMPage.getBMpageArray();

        // Conver byte array to Bitset to access individual bits
        bitMaps = BitSet.valueOf(data);
    }

    private Page pinPage(PageId pageno) throws PinPageException 
    {
        try 
        {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
            return page;
        } 
        catch (Exception e) 
        {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }


    private void unpinPage(PageId pageno, boolean dirty) throws HFBufMgrException 
    {
        try 
        {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } 
        catch (Exception e) 
        {
            throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
        }
    }

    public int get_next()
    {
        try 
        {

            if (scanCounter > counter)
            {
                PageId nextPage = currentBMPage.getNextPage();
                unpinPage(currentPageId, false);
                if (nextPage.pid != INVALID_PAGE) {
                    // Copy nextpage ID
                    currentPageId.copyPageId(nextPage);

                    Page pCurrentPage = pinPage(currentPageId);
                    currentBMPage = new BMPage(pCurrentPage);

                    // Update counter to next pages counter value
                    counter = currentBMPage.getCounter();
                    bitMaps = BitSet.valueOf(currentBMPage.getBMpageArray());

                    // Resetting scanCounter to 0 (Remember this check this part of the code)
                    scanCounter = 0;

                } 
                else 
                {
                    return -1;
                }
            }
            while (scanCounter <= counter) 
            {
                if (bitMaps.get(scanCounter) == 1) 
                {
                    int position = scanCounter;
                    scanCounter++;
                    return position;
                } 
                else 
                {
                    scanCounter++;
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return -1;
    }

    public BitSet get_next_bitmap() 
    {
        try 
        {
            if (bitMaps != null) 
            {
                BitSet currentBitMap = (BitSet) bitMaps.clone();
                PageId nextPage = currentBMPage.getNextPage();
                unpinPage(currentPageId, false);

                // Change made here
                currentPageId.copyPageId(nextPage);

                if (nextPage.pid != INVALID_PAGE) 
                {
                    Page pCurrentPage = pinPage(currentPageId);
                    currentBMPage = new BMPage(pCurrentPage);

                    counter = currentBMPage.getCounter();

                    byte[] data = currentBMPage.getBMpageArray();
                    bitMaps = BitSet.valueOf(data);

                } 
                else 
                {
                    bitMaps = null;
                }
                return currentBitMap;
            }
        } 
        catch (Exception e) 
        {
            e.printStackTrace();
        }
        return null;
    }

    public void close() throws Exception 
    {
        file.scanClose();

        if(currentPageId != null && currentPageId.pid != INVALID_PAGE)
        {
            unpinPage(currentPageId, false);
        }
    }
}