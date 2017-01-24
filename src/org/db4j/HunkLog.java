// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.nqzero.orator.Example;
import java.io.Serializable;
import java.util.Random;
import java.util.stream.Collectors;
import kilim.Pausable;
import org.apache.commons.lang3.RandomStringUtils;
import org.db4j.Db4j.Hunkable;
import org.db4j.Db4j.Hunker;
import org.db4j.Db4j.LocalInt2;
import org.db4j.Db4j.Locals;
import org.db4j.Db4j.Transaction;
import org.srlutils.Rand;
import org.srlutils.Simple;

public class HunkLog implements Hunkable<HunkLog>, Serializable {
//    static final long serialVersionUID;

    
    transient public Hunker hunker;
    public String name;
    transient public long hunksBase;
    transient public Vars loc;
    transient public int position;

    public String name() { return name; }

    public String info() {
        return "";
    }
    

    public static class Vars {
        public Locals locals = new Locals();
        public final LocalInt2 nhunks = new LocalInt2( locals );
        public final LocalInt2 kbase = new LocalInt2( locals );
    }


    public interface Loggable {
        public void restore(Hunker hunker);
    }
    

    public HunkLog set(Hunker _hunker) {
        hunker = _hunker;
        loc = new Vars();
        return this;
    }
    /**
     * initialize the array with size2: initial in bytes, and using the _name
     * only callable before hunker creation
     * fixme::useability -- should be able to add components "live"
     */
    public HunkLog init(String $name) {
        name = $name;
        hunker.register( this );
        return this;
    }

    public int create() {
        int lsize = loc.locals.size();
        return lsize;
    }
    public void createCommit(long locBase) {
        loc.locals.set(hunker, locBase);
    }
    transient int ngrow, tmpBase;
    public void postInit(Transaction tid) throws Pausable {
        ngrow = 8;
        int base = hunker.request(ngrow, true, tid)[0];
        tmpBase = base;
        hunker.put( tid, loc.nhunks.write(ngrow) );
        hunker.put(tid, loc.kbase.write(base) );
        byte [] zeros = new byte[hunker.bs];
        for (int khunk=0; khunk < ngrow; khunk++)
        {
            hunker.put(tid, (0L+khunk+base) << hunker.bb, new Command.Init());
            hunker.put(tid, (0L+khunk+base) << hunker.bb, new Command.RwBytes().init(true).set(zeros));
        }
    }
    public void postLoad(Transaction tid) throws Pausable {
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
        Example.MyKryo kryo = hunker.kryo();
        while (buffer.position() < data.length) {
            Loggable obj = (Loggable) kryo.readClassAndObject(buffer);
            if (obj==null) break;
            obj.restore(hunker);
            position = buffer.position();
        }
    }
    
    
    public static class LogFullException extends RuntimeException {}

    // the length does not get stored explicitly
    // so the caller must provide a means for detecting termination
    // eg, NUL-terminated or an included length
    public void set(int offset,byte [] data) throws LogFullException {
        if (offset + data.length > ngrow*hunker.bs)
            throw new LogFullException();
        long total = (tmpBase << hunker.bb) + offset;
        Command.RwBytes[] cmds = hunker.iocmd(null,total,data,true);
        for (Command.RwBytes cmd : cmds)
            hunker.qrunner.handleWrite(cmd);
    }
    
    public byte [] get(Transaction tid) throws Pausable {
        Command.RwInt base = hunker.put(tid, loc.kbase.read());
        Command.RwInt hunks = hunker.put(tid, loc.nhunks.read());
        tid.submitYield();
        tmpBase = base.val;
        ngrow = hunks.val;
        byte [] data = new byte[ngrow*hunker.bs];
        Command.RwBytes[] cmds = hunker.iocmd(tid,base.val << hunker.bb,data,false);
        tid.submitYield();
        return data;
    }




    public static class Chat1 extends Database {
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
            hunker.futurex(tid -> {
                int num = count.get(tid);
                users.insert(tid,num,randUser());
                count.plus(tid,1);
            });
        }
        String saved = null;
        
        public String info() {
            String val =
            hunker.future(tid -> {
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
    }

    public static void main(String[] args) {
        Chat1 hello = new Chat1();

        if (args.length==0) {
            hello.start("./db_files/hunk2.mmap",true);
            for (int ii = 0; ii < 3000; ii++)
                hello.route();
            hello.hunker.fence(null,100);
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
