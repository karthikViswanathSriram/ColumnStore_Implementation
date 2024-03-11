package columnar;
import global.*;
import java.io.*;

public class TID {
    int numRIDs;
    int position;
    RID[] recordIDs;

    public TID(int numRIDs){
        this.numRIDs = numRIDs;
        this.position = 1;
        this.recordIDs = new RID[numRIDs];
        for (int i = 0; i < numRIDs; i++) {
            this.recordIDs[i] = new RID(); // Assuming RID has a default constructor
        }
    }

    public TID(int numRIDs, int position){
        this.numRIDs = numRIDs;
        this.position = position;
        this.recordIDs = new RID[numRIDs];
        for (int i = 0; i < numRIDs; i++) {
            this.recordIDs[i] = new RID(); // Assuming RID has a default constructor
        }
    }

    public TID(int numRIDs, int position, RID[] recordIDs){
        this.numRIDs = numRIDs;
        this.position = position;
        this.recordIDs = recordIDs;
    }

    void copyTid(TID tid){
        numRIDs = tid.numRIDs;
        position = tid.position;
        recordIDs = tid.recordIDs;
    }

    boolean equals(TID tid){
        if (numRIDs==tid.numRIDs){
            return false;
        }
        if ( position==tid.position){
            return false;
        } 
        for (int i=0;i<recordIDs.length;i++){
            if (!recordIDs[i].equals(tid.recordIDs[i])){
                return false;
            }
        }
        return true;
    }

    void writeToByteArray(byte[] array, int offset)throws java.io.IOException
    {
      Convert.setIntValue ( numRIDs, offset, array);
      Convert.setIntValue ( position, offset+4, array);
      for (int i=0;i<recordIDs.length;i++){
        recordIDs[i].writeToByteArray(array, offset+8 + i*8) ;
        }
    }

    void setPosition(int position){
        this.position=position;
    }

    void setRID(int column, RID recordID) throws Exception{
        if (column<numRIDs)    
            this.recordIDs[column] = recordID;
        else
            throw new Exception("column passed exceeds number of columns in database");
    }

}
