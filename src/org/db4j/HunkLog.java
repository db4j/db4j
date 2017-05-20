// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.nqzero.orator.Example;
import java.io.Serializable;
import java.util.Random;
import kilim.Pausable;
import org.apache.commons.lang3.RandomStringUtils;
import org.db4j.Db4j.Hunkable;
import org.db4j.Db4j.LocalInt;
import org.db4j.Db4j.Locals;
import org.db4j.Db4j.Transaction;
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


    public interface Loggable {
        public void restore(Db4j db4j);
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
    protected void postInit(Transaction tid) throws Pausable {
        ngrow = 8;
        int base = db4j.request(ngrow, true, tid)[0];
        tmpBase = base;
        db4j.put( tid, loc.nhunks.write(ngrow) );
        db4j.put(tid, loc.kbase.write(base) );
        byte [] zeros = new byte[db4j.bs];
        for (int khunk=0; khunk < ngrow; khunk++)
        {
            db4j.put(tid, (0L+khunk+base) << db4j.bb, new Command.Init());
            db4j.put(tid, (0L+khunk+base) << db4j.bb, new Command.RwBytes().init(true).set(zeros));
        }
    }
    protected void postLoad(Transaction tid) throws Pausable {
        restore(tid);
    }


    public void store(Loggable obj,Example.MyKryo kryo) {
        Output buffer = new Output(2048,-1);
        kryo.writeClassAndObject(buffer,obj);
        byte [] data = buffer.toBytes();
        
        synchronized (this) {
            set(position,data);
            position += data.length;
        }
    }
    public void restore(Transaction tid) throws Pausable {
        byte [] data = get(tid);
        Input buffer = new Input(data);
        Example.MyKryo kryo = db4j.kryo();
        while (buffer.position() < data.length) {
            Loggable obj = (Loggable) kryo.readClassAndObject(buffer);
            if (obj==null) break;
            obj.restore(db4j);
            position = buffer.position();
        }
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
    
    public byte [] get(Transaction tid) throws Pausable {
        Command.RwInt base = db4j.put(tid, loc.kbase.read());
        Command.RwInt hunks = db4j.put(tid, loc.nhunks.read());
        tid.submitYield();
        tmpBase = base.val;
        ngrow = hunks.val;
        byte [] data = new byte[ngrow*db4j.bs];
        Command.RwBytes[] cmds = db4j.iocmd(tid,base.val << db4j.bb,data,false);
        tid.submitYield();
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
        
        public void route() {
            db4j.submitCall(tid -> {
                int num = count.get(tid);
                users.insert(tid,num,randUser());
                count.plus(tid,1);
            });
        }
        String saved = null;
        
        public String info() {
            String val =
            db4j.submit(tid -> {
                String last = null;
                User [] all = users.getall(tid).vals().toArray(new User[0]);
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

            if (args.length==0) {
                hello.start("./db_files/hunk2.mmap",true);
                for (int ii = 0; ii < 3000; ii++)
                    hello.route();
                hello.db4j.fence(null,100);
                hello.info();
                hello.shutdown(true);
            }
    //        else 
            {
                hello.start("./db_files/hunk2.mmap",false);
                hello.info();
            }
            System.exit(0);
        }
    }


}
