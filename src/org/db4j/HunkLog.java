// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import java.io.Serializable;
import java.util.Random;
import kilim.Pausable;
import org.apache.commons.lang3.RandomStringUtils;
import org.db4j.Db4j.Transaction;
import org.db4j.perf.DemoHunker;
import org.srlutils.Simple;

public class HunkLog extends Hunkable<HunkLog> implements Serializable {
//    static final long serialVersionUID;

    
    transient protected Db4j db4j;
    protected String name;
    transient protected Vars loc;
    transient protected int position;
    transient protected int ngrow, tmpBase;

    protected String name() { return name; }

    protected String info() {
        return "";
    }
    

    protected static class Vars {
        public Locals locals = new Locals();
        public final LocalInt nhunks = new LocalInt( locals );
        public final LocalInt kbase = new LocalInt( locals );
    }


    /**
     * a type that can be added to the log
     * @param <XX> an arg that the kryoish serializer can pass, effectively private to the kryoish
     */
    public interface Loggable<XX> {
        /** 
         * user callback to restore the object on startup
         * @param db4j the Db4j instance
         * @param local an arg that the kryoish serializer can pass, effectively private to the kryoish
         */
        public void restore(Db4j db4j,XX local);
    }
    

    protected HunkLog set(Db4j _db4j,String name) {
        db4j = _db4j;
        loc = new Vars();
        if (name != null) this.name = name;
        return this;
    }

    protected int create() {
        int lsize = loc.locals.size();
        return lsize;
    }
    protected void createCommit(long locBase) {
        loc.locals.set(db4j, locBase);
    }
    protected void postInit(Transaction txn) throws Pausable {
        ngrow = 8;
        int base = db4j.request(ngrow, true, txn)[0];
        tmpBase = base;
        db4j.put( txn, loc.nhunks.write(ngrow) );
        db4j.put(txn, loc.kbase.write(base) );
        byte [] zeros = new byte[db4j.bs];
        for (int khunk=0; khunk < ngrow; khunk++)
        {
            db4j.put(txn, (0L+khunk+base) << db4j.bb, new Command.Init());
            db4j.put(txn, (0L+khunk+base) << db4j.bb, new Command.RwBytes().init(true).set(zeros));
        }
    }
    protected void postLoad(Transaction txn) throws Pausable {
        restore(txn);
    }


    /**
     * store the object in the log
     * @param obj the object
     */
    public void store(Loggable obj) {
        byte [] data = db4j.kryoish.save(obj);
        store(data);
    }
    void store(byte [] data) {
        synchronized (this) {
            set(position,data);
            position += data.length;
        }
    }
    void restore(Transaction txn) throws Pausable {
        byte [] data = get(txn);
        position = db4j.kryoish.restore(data,position);
    }
    
    
    public static class LogFullException extends RuntimeException {}

    // the length does not get stored explicitly
    // so the caller must provide a means for detecting termination
    // eg, NUL-terminated or an included length
    public void set(int offset,byte [] data) throws LogFullException {
        if (offset + data.length > ngrow*db4j.bs)
            throw new LogFullException();
        long total = (tmpBase << db4j.bb) + offset;
        Command.RwBytes[] cmds = db4j.iocmd(null,total,data,true);
        for (Command.RwBytes cmd : cmds)
            db4j.qrunner.handleWrite(cmd);
    }
    
    public byte [] get(Transaction txn) throws Pausable {
        Command.RwInt base = db4j.put(txn, loc.kbase.read());
        Command.RwInt hunks = db4j.put(txn, loc.nhunks.read());
        txn.submitYield();
        tmpBase = base.val;
        ngrow = hunks.val;
        byte [] data = new byte[ngrow*db4j.bs];
        Command.RwBytes[] cmds = db4j.iocmd(txn,base.val << db4j.bb,data,false);
        txn.submitYield();
        return data;
    }




    static class Chat1 extends Database {
        HunkCount count;
        Btrees.IK<User> users;
        static Random src = new Random();

        public static class User {
            public String name, bio = RandomStringUtils.randomAlphanumeric(src.nextInt(8000));
            String format() { return "user: " + bio.substring(0,Math.min(10,bio.length())); }
        }
        
        static class U0 extends User { int dummy; }
        static class U1 extends User { int dummy; }
        static class U2 extends User { int dummy; }
        static class U3 extends User { int dummy; }
        static class U4 extends User { int dummy; }
        static class U5 extends User { int dummy; }
        static class U6 extends User { int dummy; }
        static class U7 extends User { int dummy; }
        static class U8 extends User { int dummy; }
        static class U9 extends User { int dummy; }
        
        
        User randUser() {
            int type = src.nextInt(10);
            switch (type) {
                case 0: return new U0();
                case 1: return new U1();
                case 2: return new U2();
                case 3: return new U3();
                case 4: return new U4();
                case 5: return new U5();
                case 6: return new U6();
                case 7: return new U7();
                case 8: return new U8();
                default:
                case 9: return new U9();
            }
        }
        
        public void route(Db4j.Connection conn) {
            conn.submitCall(txn -> {
                int num = count.get(txn);
                users.insert(txn,num,randUser());
                count.plus(txn,1);
            });
        }
        String saved = null;
        
        public String info() {
            String val =
            db4j.submit(txn -> {
                String last = null;
                User [] all = users.getall(txn).vals().toArray(new User[0]);
                for (User user : all) {
                    last = user.format();
                    if (all.length < 20) System.out.println(last);
                }
                return last;
            }).awaitb().val;
            if (saved==null) saved = val;
            else Simple.hardAssert(saved.equals(val));
            System.out.println(val);
            return null;
        }

        public static void main(String[] args) {
            Chat1 hello = new Chat1();

            String filename = DemoHunker.resolve("./db_files/hunk2.mmap");

            if (args.length==0) {
                Db4j db4j = hello.start(filename,true);
                Db4j.Connection conn = db4j.connect();
                for (int ii = 0; ii < 3000; ii++)
                    hello.route(conn);
                conn.awaitb();
                hello.info();
                hello.shutdown(true);
            }
    //        else 
            {
                hello.start(filename,false);
                hello.info();
            }
            System.exit(0);
        }
    }


}
