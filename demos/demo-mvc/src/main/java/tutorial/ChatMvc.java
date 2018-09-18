package tutorial;

import kilim.Pausable;
import kilim.http.KilimMvc;
import kilim.tools.Kilim;
import org.db4j.Btrees;
import org.db4j.Database;
import org.db4j.Db4j;
import org.db4j.Db4jMvc;
import org.db4j.Db4jMvc.Db4jRouter;
import static org.db4j.Db4jMvc.badRequest;
import org.db4j.HunkCount;
import static org.db4j.perf.DemoHunker.resolve;

public class ChatMvc extends Database {
    HunkCount count;
    Btrees.IK<User> users;
    Btrees.SI namemap;
    Btrees.IS messages;

    
    public static class User {
        public String name;
        public String bio;
    }
    
    static int toInt(String id) {
        try { return Integer.parseInt(id); }
        catch (Exception ex) { throw badRequest("invalid integer"); }
    }

    public class Router extends Db4jRouter<Router> {
        Router(KilimMvc.Clerk mk) { super(mk); }

        {
            add("/dir",() -> select(txn ->
                users.getall(txn).vals()));

            add("/get/{id}",id -> select(txn ->
                users.context().set(txn).set(toInt(id),null).get(users)).val);

            add("/list/{id}",id -> select(txn ->
                messages.findPrefix(txn,toInt(id)).vals()));

            add("/msg/{id}/{msg}",(id,msg) -> select(txn ->
                messages.insert(txn,toInt(id),msg)).val);

            add("/user/{name}",name -> select(txn ->
                namemap.find(txn,name)));
        }

        { make2("/new/{name}/{bio}",self -> self::newUser); }
        public Object newUser(String name,String bio) throws Pausable {
            User user = new User();
            user.name = name;
            user.bio = bio;
            return select(txn -> {
                Integer prev = namemap.find(txn,name);
                if (prev != null)
                    throw badRequest("user already exists");
                if (namemap.find(txn,user.name) != null) return "username is not available";
                int krow = count.plus(txn,1);
                users.insert(txn,krow,user);
                namemap.insert(txn,user.name,krow);
                return krow;
            });
        }
        
        { make0(new KilimMvc.Route(),self -> self::fallback); }
        Object fallback() throws Pausable {
            System.out.println("matter.fallback: " + req);
            return new int[0];
        }
    }
    
    public static void main(String[] args) throws Exception {
        if (Kilim.trampoline(false,args))
            return;
        ChatMvc chat = new ChatMvc();
        Db4j db4j = chat.start(resolve("../db_files/chat.mmap"),args.length > 0);
        new Db4jMvc(x -> chat.new Router(x).setup(db4j),pp -> {}).start(8081);
        System.in.read();
        System.exit(0);
    }
}
