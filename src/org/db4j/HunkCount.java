// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import java.io.Serializable;
import kilim.Pausable;
import org.db4j.Command.RwInt;
import org.db4j.Db4j.Hunkable;
import org.db4j.Db4j.Hunker;
import org.db4j.Db4j.LocalInt2;
import org.db4j.Db4j.Locals;
import org.db4j.Db4j.Transaction;

public class HunkCount implements Hunkable<HunkCount>, Serializable {
    static final long serialVersionUID = -1693345263933559568L;    
    public static class Vars {
        public Locals locals = new Locals();
        public final LocalInt2 count = new LocalInt2( locals );
    }
    transient public Vars loc;
    transient public Hunker hunker;
    public String name;

    public String info() {
        return "";
    }

    /** add delta to the stored value and return the old value */
    public int plus(Transaction tid,int delta) throws Pausable {
        RwInt read = loc.count.read();
        hunker.put( tid, read );
        if (tid.submit()) kilim.Task.yield();
        RwInt writ = loc.count.write(read.val + delta);
        hunker.put( tid, writ );
        return read.val;
    }
    public int get(Transaction tid) throws Pausable {
        RwInt read = loc.count.read();
        hunker.put( tid, read );
        if (tid.submit()) kilim.Task.yield();
        return read.val;
    }
    public void set(Transaction tid,int val) {
        RwInt writ = loc.count.write(val);
        hunker.put( tid, writ );
    }
    
    public int create() {
        int cap = loc.locals.size();
        return cap;
    }
    public void createCommit(long locBase) { loc.locals.set( hunker, locBase ); }
    public String name() { return name; }
    public HunkCount set(Hunker $hunker) {
        hunker = $hunker;
        loc = new Vars();
        return this;
    }
    public HunkCount init(String $name) { 
        hunker.register( this );
        name = $name;
        return this;
    }
    public void postInit(Transaction tid) throws Pausable {
        hunker.put(tid, loc.count.write(0));
    }
    public void postLoad(Transaction tid) throws Pausable {}
    
    public static class Demo {
        HunkCount lt;
        Hunker hunker;
        String name = "./db_files/hunk2.mmap";
        
        public class Task extends Db4j.Query {
            public void task() throws Pausable {
                int val = lt.get(tid);
                lt.set(tid,val+1);
                System.out.format( "count: %d\n", val );
            }
        }
        
        public void demo() {
            hunker = new Hunker().init( name, null );
            lt = new HunkCount();
            lt.set( hunker );
            lt.init("Hunk Count");
            hunker.create();
            for (int ii = 0; ii < 10; ii++) hunker.submit( new Task() );
            hunker.fence( null, 10 );
            lt.hunker.shutdown();
        }
    }    
    public static void main(String [] args) {
        Demo demo = new Demo();
        demo.demo();
    }
}
