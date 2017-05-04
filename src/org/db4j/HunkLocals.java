// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import kilim.Pausable;
import org.db4j.Db4j.LocalInt2;
import org.db4j.Db4j.Transaction;

public class HunkLocals extends HunkArray.I {
    public transient LocalInt2 last;

    public HunkLocals set(Db4j $db4j) {
        super.set($db4j);
        last = new LocalInt2( loc.locals );
        return this;
    }
    
    public int alloc(int index,int num,Transaction tid) throws Pausable {
        Command.RwInt cmd = last.read();
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
        klast += num;
        last.write(klast);
        set(tid,index,klast);
        return klast;
    }

    protected void postInit(Transaction tid) throws Pausable {
        super.postInit(tid);
        last.write(0);
    }
    protected void postLoad(Transaction tid) throws Pausable {}


}
