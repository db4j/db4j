// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import kilim.Pausable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.db4j.Db4j.Transaction;

// fixme:dependency - org.db4j:db4j-core should be a minimal pom
// neither lucene-analyzers nor kryo are essential to the operation of db4j, so make them optional
// for now, just including both as it's easier and the deployment to maven central for db4j-core isn't set up yet


/**
 * a db4j database table comprising an inverted index for free text search
 * note: this class depends on the lucene analyzers which is scoped::provided in this artifact, so
 * it must be included in dependent projects, eg by using org.db4j:Db4jText:LATEST
 */
public class TextSearchTable extends Database.Table {
    public HunkCount num;
    public Btrees.SI wordmap;
    public Btrees.II usages;
    public Analyzer analyzer = new StandardAnalyzer();

    public ArrayList<Integer> get(Transaction txn,String word) throws Pausable {
        Integer kword = wordmap.find(txn,word);
        if (kword==null) return null;
        ArrayList<Integer> result = new ArrayList();
        usages.findPrefix(usages.context().set(txn).set(kword,0)).visit(cc -> result.add(cc.val));
        return result;
    }

    public void put(Transaction txn,String word,Integer val) throws Pausable {
        Integer kword = wordmap.find(txn,word);
        if (kword==null) {
            kword = num.plus(txn,1);
            wordmap.insert(txn,word,kword);
        }
        usages.insert(usages.context().set(txn).set(kword,val));
    }


    public Prep prep(String doc) {
        Prep prep = new Prep();
        prep.words = parse(doc);
        return prep;
    }    
    public class Prep {
        ArrayList<String> words;
        public void add(Transaction txn,Integer id) throws Pausable {
            TextSearchTable.this.addExact(txn,words,id);
        }
    }    

    public void parseAndAdd(Transaction txn,String doc,Integer id) throws Pausable {
        ArrayList<String> words = parse(doc);
        for (String word : words)
            put(txn,word,id);
    }
    public void addExact(Transaction txn,String doc,Integer id) throws Pausable {
        put(txn,doc,id);
    }
    public void addExact(Transaction txn,ArrayList<String> doc,Integer id) throws Pausable {
        for (String word : doc)
            put(txn,word,id);
    }
    public ArrayList<Integer> search(Transaction txn,String term,boolean exact) throws Pausable {
        return exact ? searchExact(txn,term):search(txn,term);
    }
    public ArrayList<Integer> searchExact(Transaction txn,String term) throws Pausable {
        ArrayList<Integer> result = get(txn,term);
        return result==null ? new ArrayList():result;
    }
    public ArrayList<Integer> search(Transaction txn,String terms) throws Pausable {
        ArrayList<String> search = parse(terms);
        ArrayList<Integer> [] data = new ArrayList[search.size()];

        for (int ii=0; ii < search.size(); ii++)
            data[ii] = get(txn,search.get(ii));
        ArrayList<Integer> result = join(data);
        return result==null ? new ArrayList():result;
    }
    public ArrayList<String> parse(String text) {
        if (text==null || text.isEmpty())
            return new ArrayList();

        List<String> terms = tokenize(text);
        return unique(terms);
    }
    public ArrayList<String> unique(Iterable<String> terms) {
        ArrayList<String> unique = new ArrayList();
        HashSet<String> unseen = new HashSet();
        for (String stem : terms)
            if (! stop(stem) && unseen.add(stem))
                unique.add(stem);
        return unique;
    }
    public ArrayList<String> tokenize(String doc) {
        return tokenize(doc,new ArrayList());
    }
    public <TT extends Collection<String>> TT tokenize(String doc,TT result) {
        try (TokenStream raw = analyzer.tokenStream(null,doc)) {
            KStemFilter ts = new KStemFilter(raw);
            CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);
            ts.reset();
            while (ts.incrementToken())
                // fixme - strip and split at non-word chars
                result.add(term.toString());
            ts.end();
        }
        catch (IOException ex) {}
        return result;
    }

    boolean stop(String word) { return false; }

    public static <TT extends ArrayList<Integer>> TT join(TT ... lists) {
        ArrayList<Integer> valid = new ArrayList();
        for (int ii=0; ii < lists.length; ii++)
            if (lists[ii] != null)
                valid.add(ii);
        if (valid.isEmpty())
             return null;
        TT result = lists[valid.get(0)];
        if (valid.size()==1)
            return result;
        HashMap<Integer,Ibox> found = new HashMap();
        int ii = 0, last = valid.size()-1;
        for (Integer index : lists[valid.get(ii)])
            found.put(index,new Ibox(1));
        for (ii=1; ii < last; ii++)
            for (Integer index : lists[valid.get(ii)]) {
                Ibox box = found.get(index);
                if (box != null) box.val++;
            }
        result.clear();
        for (Integer index : lists[valid.get(last)]) {
            Ibox box = found.get(index);
            if (box != null && box.val==last)
                result.add(index);
        }
        return result;
    }

    public static class Ibox {
        public int val;
        public Ibox() {};
        public Ibox(int $val) { val = $val; };
    }
}
