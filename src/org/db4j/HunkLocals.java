// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import kilim.Pausable;
import org.db4j.Db4j.LocalInt2;
import org.db4j.Db4j.Transaction;

/**
 * a map from an integer index to variable-sized byte arrays,
 * implemented as an array with attached auxiliary pages
 */
public class HunkLocals extends HunkArray.I {
    public transient LocalInt2 last;

    protected HunkLocals set(Db4j $db4j,String name) {
        super.set($db4j,name);
        last = new LocalInt2( loc.locals );
        return this;
    }

    /**
     * allocate num contiguous bytes in the auxiliary space.
     * store the starting offset in the array at index and return it.
     * note: requests that do not fit in the current page leave a hole at the end of the current page.
     * reallocating for an existing index leaks the old allocation
     * @param index the index in the array
     * @param num the number of bytes to allocate
     * @param tid the transaction
     * @return the starting offset in the auxiliary space
     */    
    public int alloc(int index,int num,Transaction tid) throws Pausable {
        Command.RwInt cmd = last.read().add(db4j,tid);
        if (tid.submit()) kilim.Task.yield();
        int klast = cmd.val;
        int chunk = (klast-1) >> db4j.bb;
        int nhunks = (klast+num-1) >> db4j.bb;
        int n2 = nhunks - chunk;
        if (n2 > 0) {
            n2 = (num+db4j.bs-1) >> db4j.bb;
            int khunk = db4j.request(n2,true,tid)[0];
            klast = khunk << db4j.bb;
            for (int k1 = 0; k1 < n2; k1++)
                db4j.put( tid, (khunk+k1) << db4j.bb, new Command.Init() );
        }
        last.write(klast+num).add(db4j,tid);
        set(tid,index,klast);
        return klast;
    }

    protected void postInit(Transaction tid) throws Pausable {
        super.postInit(tid);
        last.write(0).add(db4j,tid);
    }
    protected void postLoad(Transaction tid) throws Pausable {}


}
