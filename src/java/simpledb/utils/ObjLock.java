package simpledb.utils;

import simpledb.PageId;
import simpledb.TransactionId;
import simpledb.utils.LockType;

import java.util.ArrayList;

class ObjLock {
    // boolean blocked;
    LockType type;
    PageId obj;
    ArrayList<TransactionId> holders;


    public ObjLock(LockType t, PageId obj, ArrayList<TransactionId> holders) {
        this.type = t;
        this.obj = obj;
        this.holders = holders;
    }

    public void setType(LockType type) {
        this.type = type;
    }

    public LockType getType() {
        return type;
    }

    public PageId getObj() {
        return obj;
    }

    public ArrayList<TransactionId> getHolders() {
        return holders;
    }

    public boolean tryUpgradeLock(TransactionId tid) {
        if (type == LockType.SLock && holders.size() == 1 && holders.get(0).equals(tid)) {
            type = LockType.XLock;
            return true;
        }
        return false;
    }

    public TransactionId addHolder(TransactionId tid) {
        if (type == LockType.SLock) {
            if (!holders.contains(tid)) {
                holders.add(tid);
            }
            return tid;
        }
        return null;
    }
}
