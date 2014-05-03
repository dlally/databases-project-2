package simpledb.index.ehash.Trie;

import simpledb.query.Constant;
import simpledb.record.RID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple trie class for use with extensible hashing.
 * Created by Doug Lally & Nate Miller on 5/3/2014.
 */
public class TrieNode {
    // Maximum size of each bucket
    private final int BUCKET_SIZE = 50;
    private TrieNode left, right;
    private ConcurrentHashMap<Constant, ArrayList<RID>> data;

    public TrieNode(){
        this.left = null;
        this.right = null;
        data = new ConcurrentHashMap<Constant, ArrayList<RID>>();
    }

    public TrieNode left(){
        return left;
    }

    public TrieNode right(){
        return right;
    }

    public ArrayList<RID> get(Constant constant){
        return data.get(constant);
    }
    // Insert a value. If we need to, add children first
    public void put(Constant val, RID rid){
        ArrayList<RID> values = data.get(val);
        if(values == null){
            values = new ArrayList<RID>();
        }
        values.add(rid);
        data.put(val, values);
    }

    // Remove this value for the RID. If there are no more values for this RID, delete from the table
    public void remove(Constant val, RID rid){
        ArrayList<RID> values = data.get(val);
        values.remove(rid);
        if(values.isEmpty()){
            data.remove(val);
        }
    }
    //Get all data contained in this node
    public ConcurrentHashMap<Constant, ArrayList<RID>> getDataSet(){
        return data;
    }

    // Add children
    public void grow(){
        if(left == null){
            left = new TrieNode();
        }
        if(right == null) {
            right = new TrieNode();
        }

    }
    //Remove the data stored from this node
    public void deleteData(){
        data = null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if(data != null) {
            sb.append("###Data for this node###\n");
            sb.append(Arrays.toString(data.keySet().toArray()));
        }
        else{
            sb.append("Leaf node, no data\n");
        }
        if(left != null){
            sb.append("Left child:\n" + left.toString() + "\n");
        }
        if(right != null){
            sb.append("Right child:\n" + right.toString());
        }
        return sb.toString();
    }
}
