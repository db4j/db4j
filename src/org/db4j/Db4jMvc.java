package org.db4j;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.function.Consumer;
import kilim.Pausable;
import kilim.http.HttpRequest;
import kilim.http.HttpResponse;
import kilim.http.KilimMvc;

public class Db4jMvc extends KilimMvc {
    boolean yoda = true;
    static Gson gsonStatic = new Gson();
    public Gson gson = gsonStatic;
    static JsonParser parser = new JsonParser();

    public <PP extends Db4jRouter> Db4jMvc(Scannable<PP> source,Preppable<PP> auth) {
        scan(source,auth);
    }

    public kilim.http.HttpServer start(int port) throws IOException {
        return new kilim.http.HttpServer(port,() -> new Session(this::handle));
    }
    protected <PP extends Router> PP supply(Scannable<PP> source,Consumer<Route> router) {
        // fixme - make KilimMvc generic
        PP pp = super.supply(source,router);
        ((Db4jRouter) pp).gson = gson;
        return pp;
    }

    public static class Db4jRouter<PP extends Db4jRouter> extends Router<PP> {
        public Db4j db4j;
        public Gson gson;
        protected boolean logExtra = false;

        protected Db4jRouter(Consumer<Route> mk) { super(mk); }

        public PP setup(Db4j $db4j) {
            db4j = $db4j;
            return (PP) this;
        }
        public <TT> TT select(Db4j.Utils.QueryFunction<TT> body) throws Pausable {
            return db4j.submit(body).await().val;
        }
        public void call(Db4j.Utils.QueryCallable body) throws Pausable {
            db4j.submitCall(body).await();
        }

        public Gson gson() { return gson; }
        public <TT> TT fromJson(String txt,Class<TT> klass) { return gson().fromJson(txt,klass); }
        public String toJson(Object obj) { return gson().toJson(obj); }
        public Object parseJson(String txt) { return parser.parse(txt); }
        public void logExtra(String txt,Object val) {
            if (! logExtra)
                return;
            Object parsed = parseJson(txt);
            String v1 = gsonStatic.toJson(val);
            String v2 = gsonStatic.toJson(parsed);
            if (! v1.equals(v2)) {
                System.out.format("%-40s --> %s\n",req.uriPath,val.getClass().getName());
                System.out.println("\t" + v1);
                System.out.println("\t" + v2);
                System.out.println("\t" + txt);
            }
        }
        
        public <TT> TT body(Class<TT> klass) {
            String txt = body();
            TT val = fromJson(txt,klass);
            logExtra(txt,val);
            return val;
        }
        public String body() {
            return req.extractRange(req.contentOffset,req.contentOffset+req.contentLength);
        }
        public byte [] rawBody() {
            return req.extractBytes(req.contentOffset,req.contentOffset+req.contentLength);
        }
    }

    public Gson gson() { return gson; }
    public void write(HttpResponse resp,Object obj) throws IOException {
        byte[] msg;
        if (obj instanceof String) msg = ((String) obj).getBytes();
        else if (obj instanceof byte[]) msg = (byte[]) obj;
        else msg = gson().toJson(obj).getBytes();
        sendJson(resp,msg);
    }
    public static class HttpStatus extends RuntimeException {
        byte [] status;
        public HttpStatus(String message,byte [] $status) {
            super(message);
            status = $status;
        }
        public Object route(HttpResponse resp) {
            resp.status = status;
            return getMessage();
        }
    }
    public static HttpStatus badRequest(String message) {
        return new HttpStatus(message,HttpResponse.ST_BAD_REQUEST);
    }
    public Object handleEx(Session session,HttpRequest req,HttpResponse resp,Exception ex) {
        if (ex instanceof HttpStatus)
            return ((HttpStatus) ex).route(resp);
        resp.status = HttpResponse.ST_BAD_REQUEST;
        return ex.getMessage();
    }
    public void handle(Session session,HttpRequest req,HttpResponse resp) throws Pausable, Exception {
        Object reply;

        if (yoda)
            try { reply = route(session,req,resp); }
            catch (HttpStatus ex) { reply = handleEx(session,req,resp,ex); }
        else
            try { reply = route(session,req,resp); }
            catch (Exception ex) { reply = handleEx(session,req,resp,ex); }

        if (reply != null) {
            write(resp,reply);
            session.sendResponse(resp);
        }
    }
}
