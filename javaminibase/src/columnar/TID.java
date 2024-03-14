package columnar;

import global.Convert;
import global.RID;

public class TID {
    int numRIDs;
    int position;
    RID[] recordIDs;

    public TID(){}
    public TID(int numRids){
        this.numRIDs = numRids;
    }
    public TID(int numRids, int position){
    	this.numRIDs = numRids;
        this.position = position;
    }
    public TID(int numRids, int position, RID[] recordIDs){
    	this.numRIDs = numRids;
        this.position = position;
        this.recordIDs = new RID[numRids];
        for(int i=0;i<recordIDs.length;i++){
            this.recordIDs[i] = new RID();
            this.recordIDs[i].copyRid(recordIDs[i]);
        }
    }

    public void copyTid(TID tid){
        numRIDs = tid.numRIDs;
        position = tid.position;
        recordIDs = new RID[numRIDs];
        for(int i = 0; i< numRIDs; i++){
            recordIDs[i].copyRid(tid.recordIDs[i]);
        }
    }

    public boolean equals(TID tid){

        if(numRIDs != tid.numRIDs) 
        	return false;
        if(position != tid.position) 
        	return false;
        for(int i = 0; i< numRIDs; i++){
            if(!recordIDs[i].equals(tid.recordIDs[i])) 
            	return false;
        }
        return true;
    }

    public void writeToByteArray(byte[] array, int offset) throws java.io.IOException
    {
        Convert.setIntValue ( numRIDs, offset, array);
        Convert.setIntValue ( position, offset+4, array);

        for(int i = 0; i< numRIDs; i++){
            offset = offset + 8;
            recordIDs[i].writeToByteArray(array, offset);
        }
    }

    public void setPosition(int position){

        this.position = position;
    }

    public void setRID(int column, RID recordID){

        if(column < numRIDs){
            recordIDs[column].copyRid(recordID);
        }
    }

    public int getPosition() {
        return position;
    }
}