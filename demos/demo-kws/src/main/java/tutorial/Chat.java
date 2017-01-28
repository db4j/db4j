// copyright 2017 nqzero - see License.txt for terms

package tutorial;

import kilim.Pausable;
import java.io.Serializable;
import java.util.stream.Collectors;
import org.db4j.Btrees;
import org.db4j.Database;
import org.db4j.HunkCount;
import org.apache.commons.lang3.RandomStringUtils;
import org.srlutils.Rand;

public class Chat extends Database {
    HunkCount count;
    Btrees.IO<User> users;
    Btrees.SI namemap;
    Btrees.IS messages;
    public static class User implements Serializable {
        public String name, bio;
        public User(String name,String bio) { this.name=name; this.bio=bio; }
        String format() { return "user: " + name + " <" + bio + ">"; }
    }    
    public String route(String query) throws Pausable {
        String cmds[]=query.split("/"), cmd=cmds.length > 1 ? cmds[1]:"none";
        Integer id = parse(cmds,2);
        return offer(tid -> { switch (cmd) {
            case "dir" : return users.getall(tid).vals().stream().map(User::format).collect(Collectors.joining("\n"));
            case "get" : return users.context().set(tid).set(id,null).get(users).val.format();
            case "list": return messages.findPrefix(tid,id).vals().stream().collect(Collectors.joining("\n"));
            case "msg" : return "sent: " + messages.insert(tid,id,cmds[3]).val;
            case "user": return "" + namemap.find(tid,cmds[2]);
            case "new" : 
                    User user = new User(cmds[2],cmds[3]);
                    int krow = count.plus(tid,1);
                    users.insert(tid,krow,user);
                    namemap.insert(tid,user.name,krow);
                    return "" + krow;
            case "random":
                int num = count.get(tid), rid = Rand.source.nextInt(0,num);
                if (num > 0)
                    messages.insert(tid,rid,RandomStringUtils.randomAscii(33));
                return "random insert";
            default: return "";
        }});
    }
    public static void main(String[] args) throws Exception {
        Chat chat = new Chat();
        chat.start("../db_files/hunk2.mmap",args.length > 0);
        new kilim.http.HttpServer(8080, req -> chat.route(req.uriPath)+"\n");
        System.in.read();
        System.exit(0);
    }
    static Integer parse(String vals[],int index) {
        try { return Integer.parseInt(vals[index]); } catch(Exception ex) { return 0; }
    }
}
