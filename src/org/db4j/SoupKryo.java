package org.db4j;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.nqzero.orator.Example;
import com.nqzero.orator.KryoInjector;
import org.db4j.HunkLog.Loggable;

public class SoupKryo implements Db4j.Kryoish {
    Example.MyKryo kryo;
    KryoFactory kryoFactory;
    KryoPool kryoPool;
    Db4j db4j;

    Example.MyKryo kryo() { return ((Example.MyKryo) kryoPool.borrow()).pool(kryoPool); }

    public SoupKryo init(Db4j $db4j) {
        db4j = $db4j;
        kryo = new Example.MyKryo(new HunkResolver()).init();
        doRegistration(kryo);
        kryoFactory = new KryoFactory() {
            public Kryo create() { return kryo.dup(); }
        };
        kryoPool = new KryoPool.Builder(kryoFactory).build();
        return this;
    }
    
    void doRegistration(Kryo kryo) {
        kryo.register(RegPair.class);
        kryo.register(Btrees.IS.class);
        KryoInjector.init(kryo);
    }
    class HunkResolver extends Example.Resolver {
        public synchronized Registration registerImplicit(Class type) {
            Registration reg = getRegistration(type);
            if (reg != null)
                return reg;
            int id = kryo.getNextRegistrationId();
            reg = new Registration(type, kryo.getDefaultSerializer(type), id);
            RegPair pair = new RegPair();
            pair.id = id;
            pair.name = type.getName();
            byte [] data = store(pair,(Example.MyKryo) kryo);
            db4j.logStore.store(data);
            System.out.format("RegPair.store: %4d %s\n",id,pair.name);
            return register(reg);
        }
        public byte [] store(Object obj,Example.MyKryo local) {
            Output buffer = new Output(2048,-1);
            local.writeClassAndObject(buffer,obj);
            byte [] data = buffer.toBytes();
            return data;
        }

    }


    static class RegPair implements Loggable<Example.MyKryo> {
        int id;
        String name;
        public void restore(Db4j db4j,Example.MyKryo local) {
            try {
                System.out.format("RegPair.restore: %4d %s\n",id,name);
                Class type = Class.forName(name);
                local.register(type,id);
            }
            catch (ClassNotFoundException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
    // https://stackoverflow.com/questions/4394978/copy-fields-between-similar-classes-in-java
    
    public <TT> void copy(TT src,TT dst,boolean copyTrans) {
        Example.MyKryo local = kryo();
        KryoInjector.copy(local,src,dst,copyTrans);
        local.unpool();
    }
    public void register(Class type,int id) {
        Example.MyKryo local = kryo();
        local.register(type,id);
        local.unpool();
    }

    public int restore(byte [] data,int position) {
        Input buffer = new Input(data);
        Example.MyKryo local = kryo();
        while (buffer.position() < data.length) {
            Loggable obj = (Loggable) local.readClassAndObject(buffer);
            if (obj==null) break;
            obj.restore(db4j,local);
            position = buffer.position();
        }
        local.unpool();
        return position;
    }
    public <TT> TT convert(byte[] bytes) {
        Input input = new Input(bytes);
        return (TT) kryo().get(input);
    }
    public <TT> byte [] save(TT val) {
        // fixme - will the buffer auto-resize ? can it throw exception causing unpool to get skipped ?
        Output buffer = new Output(2048,-1);
        kryo().put(buffer,val,false);
        return buffer.toBytes();
    }
    
}
