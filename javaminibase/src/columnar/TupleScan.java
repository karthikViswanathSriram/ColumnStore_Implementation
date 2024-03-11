package columnar;

import java.io.IOException;

import heap.InvalidTupleSizeException;
import heap.Scan;

public class TupleScan {
    private Columnarfile cf;
    private int ncols;
    Scan[] sc;

    public TupleScan(Columnarfile f) throws InvalidTupleSizeException, IOException{
        cf = f;
        ncols = f.get_numcols();
        for(int i=1;i<ncols+1;i++){
            sc[i-1] = f.get_HF(i).openScan();
        }
    }

    public void closetuplescan(){
        for(int i=0;i<ncols;i++){
            sc[i].closescan();
        }
    }
}
