
import java.io.Serializable;
import org.db4j.Btrees;
import org.db4j.Database;
import org.db4j.Db4j;
import org.db4j.HunkCount;
import static org.db4j.perf.DemoHunker.resolve;

/** a demo of using kilim and db4j without weaving - the trampoline in main handles it */
public class DemoTrampoline extends Database {
    HunkCount count;
    Btrees.SI namemap;
    Btrees.IS messages;
    Btrees.IO<User> users;
    Db4j.Connection conn;
    
    public static class User implements Serializable {
        public String name, bio;
        public User(String name,String bio) { this.name=name; this.bio=bio; }
        String format() { return "user: " + name + " <" + bio + ">"; }
    }
    void add(String name,String bio) {
        conn.submit(txn -> {
            User user = new User(name,bio);
            int krow = count.plus(txn,1);
            users.insert(txn,krow,user);
            namemap.insert(txn,user.name,krow);
            return krow;
        });
    }
    void check() {
        conn.submitCall(txn -> {
            int krow = namemap.find(txn,"static");
            System.out.println(users.find(txn,krow).format());
        }).awaitb();
    }
    public static void main(String[] args) throws Exception {
        if (kilim.tools.Kilim.trampoline(false,args)) return;
        
        DemoTrampoline demo = new DemoTrampoline();
        demo.start(resolve("../db_files/chat.mmap"),true);
        demo.conn = demo.db4j.connect();
        
        demo.add("kernigan","hello world");
        demo.add("static","main");
        demo.add("paine","blood of patriots");
        
        for (int ii=0; ii < 100000; ii++)
            demo.add("tyrant_"+ii,"democrat or republican "+ii);
        
        
        demo.check();

        
        demo.shutdown(true);

    }
}
