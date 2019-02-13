// copyright 2017 nqzero - see License.txt for terms

package tutorial;

import com.nqzero.orator.Orator;
import kilim.Pausable;
import java.io.Serializable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.db4j.Btrees;
import org.db4j.Database;
import org.db4j.HunkCount;
import org.apache.commons.lang3.RandomStringUtils;
import org.db4j.Db4j;
import static org.db4j.perf.DemoHunker.resolve;
import org.srlutils.Rand;

public class Chat extends Database {
    public HunkCount count;
    public Btrees.IO<User> users;
    public Btrees.SI namemap;
    public Btrees.IS messages;
    public static class User implements Serializable {
        public String name, bio;
        public User() {}
        public User(String name,String bio) { this.name=name; this.bio=bio; }
        public String format() { return "user: " + name + " <" + bio + ">"; }
    }    
    public String route(String query) throws Pausable {
        String cmds[]=query.split("/"), cmd=cmds.length > 1 ? cmds[1]:"none";
        Integer id = parse(cmds,2);
        return db4j.submit(txn -> { switch (cmd) {
            case "dir" : return print(users.getall(txn).vals(), User::format);
            case "list": return print(messages.findPrefix(txn,id).vals(), x -> x);
            case "get" : return users.find(txn,id).format();
            case "msg" : return "sent: " + messages.insert(txn,id,cmds[3]).val;
            case "user": return "" + namemap.find(txn,cmds[2]);
            case "new" : 
                    User user = new User(cmds[2],cmds[3]);
                    int krow = count.plus(txn,1);
                    users.insert(txn,krow,user);
                    namemap.insert(txn,user.name,krow);
                    return "" + krow;
            case "random":
                int num = count.get(txn), rid = Rand.source.nextInt(0,num);
                if (num > 0)
                    messages.insert(txn,rid,RandomStringUtils.randomAscii(33));
                return "random insert";
            default: return "";
        }
        }).await().val;
    }
    static Chat global;
    public static Chat chat() { return global; }
    public Db4j db4j() { return db4j; }
    
    public static void main(String[] args) throws Exception {
        Chat chat = new Chat();
        chat.start(resolve("./db_files/chat.mmap"),args.length > 0);
        new kilim.http.HttpServer(8080, req -> chat.route(req.uriPath)+"\n");
        global = chat;
        new Orator().init(8081);
        System.in.read();
        System.exit(0);
    }
    static <TT> String print(List<TT> vals,Function<TT,String> mapping) {
        return vals.stream().map(mapping).collect(Collectors.joining("\n")); }
    static Integer parse(String vals[],int index) {
        try { return Integer.parseInt(vals[index]); } catch(Exception ex) { return 0; }
    }
}
