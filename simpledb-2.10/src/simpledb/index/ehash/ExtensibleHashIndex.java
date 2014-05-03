package simpledb.index.ehash;

import simpledb.index.Index;
import simpledb.index.ehash.Trie.TrieNode;
import simpledb.query.Constant;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.Set;

/**
 * Implementation of an Extensible Hash Index as covered in class
 * This could be further optimised by keeping track of depths to allow
 * faster refactoring when adding buckets
 * Created by Doug Lally & Nate Miller on 5/3/2014.
 */
public class ExtensibleHashIndex implements Index {


    private final int BUCKET_SIZE = 3; // Arbitrary limit on bucket size
    private String  binaryString;
    private Constant searchkey;
    private TrieNode root, currNode;
    private int index;

    public ExtensibleHashIndex(String idxname, Schema sch, Transaction tx){
        // I dont see a use for the vars passed in, so i'll just ignore them
        this.searchkey = null;
        root = new TrieNode();
        currNode = root;
        index = 0;

    }



    /**
     * Positions the index before the first record
     * having the specified search key.
     *
     * @param searchkey the search key value.
     */
    @Override
    public void beforeFirst(Constant searchkey) {
        close();
        this.searchkey = searchkey;
        binaryString = Integer.toBinaryString(searchkey.hashCode());
        // Walk the trie
        currNode = walk(binaryString);
    }

    /**
     * Moves the index to the next record having the
     * search key specified in the beforeFirst method.
     * Returns false if there are no more such index records.
     *
     * @return false if no other index records have the search key.
     */
    @Override
    public boolean next() {
        index++;
        if(currNode.get(searchkey).size() > index){
            return true;
        }
        return false;
    }

    /**
     * Returns the dataRID value stored in the current index record.
     *
     * @return the dataRID stored in the current index record.
     */
    @Override
    public RID getDataRid() {
        return currNode.get(searchkey).get(index);
    }

    /**
     * Inserts an index record having the specified
     * dataval and dataRID values.
     *
     * @param dataval the dataval in the new index record.
     * @param datarid the dataRID in the new index record.
     */
    @Override
    public void insert(Constant dataval, RID datarid) {
        String bString = Integer.toBinaryString(dataval.hashCode());
        TrieNode node = walk(bString);
        //Check if we need to extend our index
        if(node.getDataSet().keySet().size() >= BUCKET_SIZE){
            //Add children
            node.grow();
            Set<Constant> keySet = node.getDataSet().keySet();
            // For each of the values, figure out where all the children now go
            for(Constant c : keySet){
                ArrayList<RID> rids = node.getDataSet().get(c);
                for(RID r : rids){
                    insert(c, r);
                }
            }
            node.deleteData();
            //Depth has changed, figure out correct node now
            node = walk(bString);
        }
        //Insert the value
        node.put(dataval, datarid);


    }

    /**
     * Deletes the index record having the specified
     * dataval and dataRID values.
     *
     * @param dataval the dataval of the deleted index record
     * @param datarid the dataRID of the deleted index record
     */
    @Override
    public void delete(Constant dataval, RID datarid) {
        //Find the right node
        String bString = Integer.toBinaryString(dataval.hashCode());
        TrieNode node = walk(bString);
        //Remove the entry
        node.remove(dataval, datarid);


    }

    /**
     * Closes the index.
     */
    @Override
    public void close() {
        index = 0;
        currNode = root;

    }

    public TrieNode walk(String binaryString){
        TrieNode foundNode = root;
        for(int i = 0; i < binaryString.length(); i++){
            if(binaryString.charAt(i) == '0'){
                if(foundNode.left() != null){
                    foundNode = foundNode.left();
                }
                else{
                    //Reached max depth
                    break;
                }
            }
            else{
                if(foundNode.right() != null){
                    foundNode = foundNode.right();
                }
                else{
                    //Reached max depth
                    break;
                }
            }
        }
        return foundNode;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("###Begin EHI Info###\n");
        sb.append(root.toString());
        return sb.toString();
    }
}