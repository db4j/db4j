package tutorial;

import com.nqzero.orator.Orator;
import com.nqzero.orator.OratorUtils;
import com.nqzero.orator.OratorUtils.Taskable;
import kilim.Mailbox;
import kilim.Pausable;
import org.srlutils.Rand;
import tutorial.Chat.User;

public class Douglas implements Taskable<Chat.User> {
        
    public Chat.User task() throws Pausable {
        Chat chat = Chat.global;
        return chat.db4j().submit(txn -> {
            int num = chat.count.get(txn);
            int rid = Rand.source.nextInt(0,num);
            return chat.users.find(txn,rid);
        }).await().val;
    }
    
    
    public static void main(String[] args) throws Exception {
        int port = 8081;

        Orator net = new Orator();
        net.init(0);

        OratorUtils.Remote roa = net.remotify(new OratorUtils.Remote().set(null,0).inet,port);
        OratorUtils.Nest root = net.kellyify(0,true,roa);

        if (args.length==0) {
            new kilim.http.HttpServer(8083, req -> {
                Mailbox<User> box = net.sendTask(new Douglas(),root);
                User user = box.get(10000);
                if (user==null) {
                    System.out.println("query stalled");
                    throw new RuntimeException("reply timed out");
                }
                else
                    return user.format()+"\n";
            });
            System.in.read();
            net.boxMap.forEachKey(0,kiss -> {
                System.out.println("kiss: " + kiss);
            });
            System.exit(0);
        }
        
        

        int num = 1000;
        try { num = Integer.parseInt(args[0]); }
        catch (Throwable ex) {}
        System.out.println("num: " + num);
        

        for (int jj=0; jj < 10; jj++) {
            Mailbox<Chat.User> box [] = new Mailbox[num];
            for (int ii=0; ii < num; ii++)
                box[ii] = net.sendTask(new Douglas(),root);
            System.out.println("requests sent");
            for (int ii=0; ii < num-1; ii++)
                box[ii].getb();
            System.out.println(box[num-1].getb().format());
        }
        System.exit(0);
    }
    
}
