// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import java.lang.reflect.Field;
import org.srlutils.Simple;
import static org.srlutils.Simple.Reflect.getFields;

/** a generic database that takes it's configuration from it's variables */
public class Database {
    public Db4j db4j;
    Table [] tables;
    Self self = new Self();
    Thread shutdownThread;

    static class Self extends Table {
        public Self() {}
    }

    static String base(Field field) {
        return field.getDeclaringClass().getSimpleName() + "/";
    }

    public static <TT extends Database> TT start(TT database,String filename,boolean build) {
        database.start(filename,build);
        return database;
    }
    
    /** 
     * build all the tables in the database
     * @param db4j the already initialized instance
     * @param overwrite if set, overwrite existing columns
     */
    public void build(Db4j db4j,boolean overwrite) {
        this.db4j = db4j;
        Field [] fields = getFields(this.getClass(),Database.class,Table.class);
        tables = new Table[ fields.length ];
        int ktable = 0;
        for (Field field : fields) {
            Table table;
            table = (Table) Simple.Reflect.alloc(field.getType(),false );
            tables[ ktable++ ] = table;
            table.init(base(field), db4j );
            table.build(table,overwrite);
            Simple.Reflect.set( this, field.getName(), table );
        }
        self.init(null,db4j);
        self.build(this,overwrite);
    }
    public synchronized void shutdown(boolean orig) {
        // fixme - synchronized but db4j.shutdown() doesn't appear to be safe to call twice
        //         there needs to be some sort of guard in place
        if (orig && shutdownThread != null)
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
        shutdownThread = null;
        db4j.shutdown();
    }
    /**
     * initialize db4j and either build or load the database
     * @param filename the name of the file to use to initialize db4j
     * @param build if set, build the tables, overwriting any existing columns
     * @return this
     */
    public Database start(String filename,boolean build) {
        Db4j local = build ? new Db4j() : Db4j.load(filename );
        local.userClassLoader = this.getClass().getClassLoader();
        if (build) {
            local.init(filename, -(2L<<30) );
            build(local,true);
            local.create();
        }
        else load(local);
        shutdownThread = new Thread(new Runnable() {
            public void run() {
                shutdown(false);
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownThread);
        return this;
    }
    /** 
     * load all the tables in the database
     * @param db4j the already initialized instance
     */
    public void load(Db4j db4j) {
        this.db4j = db4j;
        Field [] fields = getFields(this.getClass(),Database.class,Table.class);
        tables = new Table[ fields.length ];
        int ktable = 0;
        for (Field field : fields) {
            Table table = (Table) Simple.Reflect.alloc(field.getType(),false );
            table.init(base(field), db4j );
            table.load(table);
            tables[ ktable++ ] = table;
            Simple.Reflect.set( this, field.getName(), table );
        }
        self.init(null,db4j);
        self.load(this);
    }

    
    
    
    /** a collection of columns */
    public static class Table {
        String root;
        public Db4j db4j;
        Hunkable [] columns;
        Table init(String _root,Db4j db4j) {
            this.db4j = db4j;
            root = _root;
            return this;
        }
        void build(Object source,boolean overwrite) {
            Field [] fields = getFields(source.getClass(),Object.class,Hunkable.class);
            columns = new Hunkable[ fields.length ];
            int ktable = 0;
            for (Field field : fields) {
                String name = filename(field);
                Hunkable composite = db4j.guts.lookup( name );
                if (overwrite || composite == null) {
                    composite = (Hunkable) Simple.Reflect.alloc(field.getType(),false );
                    db4j.register(composite,name);
                }
                columns[ ktable++ ] = composite;
                Simple.Reflect.set(source, field.getName(), composite);
            }
        }

        void load(Object source) {
            Field [] fields = getFields(source.getClass(),Object.class,Hunkable.class);
            columns = new Hunkable[ fields.length ];
            int kcol = 0;
            for (Field field : fields) {
                Hunkable composite = null;
                String filename = filename(field);
                composite = (Hunkable) db4j.guts.lookup( filename );
                if (composite == null) {
                    String err = String.format( "failed to find Hunkable: %s, as field: %s",
                            filename, field.getName() );
                    throw new RuntimeException( err );
                }
                columns[ kcol++ ] = composite;
                Simple.Reflect.set(source, field.getName(), composite);
            }
        }
        String filename(Field field) {
            String base = root==null ? base(field) : root;
            String table = "/" + this.getClass().getSimpleName() + "/";
            return base + table + field.getName();
        }
    }




}











