// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import java.io.Serializable;
import kilim.Pausable;
import org.db4j.Command.RwInt;
import org.db4j.Db4j.Transaction;
import org.db4j.perf.DemoHunker;

public class HunkCount extends Hunkable<HunkCount> implements Serializable {
    static final long serialVersionUID = -1693345263933559568L;    
    protected static class Vars {
        public Locals locals = new Locals();
        public final LocalInt count = new LocalInt( locals );
    }
    transient protected Vars loc;
    transient protected Db4j db4j;
    protected String name;

    protected String info() {
        return "";
    }

    /** add delta to the stored value and return the old value */
    public int plus(Transaction txn,int delta) throws Pausable {
        RwInt read = loc.count.read();
        db4j.put( txn, read );
        if (txn.submit()) kilim.Task.yield();
        RwInt writ = loc.count.write(read.val + delta);
        db4j.put( txn, writ );
        return read.val;
    }
    public int get(Transaction txn) throws Pausable {
        RwInt read = loc.count.read();
        db4j.put( txn, read );
        if (txn.submit()) kilim.Task.yield();
        return read.val;
    }
    public void set(Transaction txn,int val) {
        RwInt writ = loc.count.write(val);
        db4j.put( txn, writ );
    }
    
    protected int create() {
        int cap = loc.locals.size();
        return cap;
    }
    protected void createCommit(long locBase) { loc.locals.set(db4j, locBase ); }
    protected String name() { return name; }
    protected HunkCount set(Db4j $db4j,String name) {
        db4j = $db4j;
        loc = new Vars();
        if (name != null) this.name = name;
        return this;
    }
    protected void postInit(Transaction txn) throws Pausable {
        db4j.put(txn, loc.count.write(0));
    }
    protected void postLoad(Transaction txn) throws Pausable {}
    
    static class Demo {
        HunkCount lt;
        Db4j db4j;
        String name = DemoHunker.resolve("./db_files/hunk2.mmap");
        String path = Demo.class.getName() + "/lt";
        
        public class Task extends Db4j.Query {
            public void task() throws Pausable {
                int val = lt.get(txn);
                lt.set(txn,val+1);
                System.out.format( "count: %d\n", val );
            }
        }
        
        public void demo() {
            db4j = new Db4j().init(name, null);
            Db4j.Connection conn = db4j.connect();
            lt = db4j.submit(txn -> db4j.create(txn, new HunkCount(), path)).awaitb().val;
            for (int ii = 0; ii < 10; ii++) conn.submitQuery( new Task() );
            conn.awaitb();
            lt.db4j.shutdown();
        }

        public static void main(String [] args) {
            Demo demo = new Demo();
            demo.demo();
        }
    }    
}
