package simpledb.parse;

import simpledb.index.IndexType;

/**
 * The parser for the <i>create index</i> statement.
 * @author Edward Sciore
 */
public class CreateIndexData {
   private String idxname, tblname, fldname;
   private IndexType idxtype;

    public IndexType getIdxtype() {
        return idxtype;
    }


    public String getIdxname() {
        return idxname;
    }


    public String getTblname() {
        return tblname;
    }


    public String getFldname() {
        return fldname;
    }

    /**
    * Saves the table and field names of the specified index.
    */
   public CreateIndexData(IndexType idxtype, String idxname, String tblname, String fldname) {
      this.idxtype = idxtype;
      this.idxname = idxname;
      this.tblname = tblname;
      this.fldname = fldname;
   }
   
   /**
    * Returns the name of the index.
    * @return the name of the index
    */
   public String indexName() {
      return idxname;
   }
   
   /**
    * Returns the name of the indexed table.
    * @return the name of the indexed table
    */
   public String tableName() {
      return tblname;
   }
   
   /**
    * Returns the name of the indexed field.
    * @return the name of the indexed field
    */
   public String fieldName() {
      return fldname;
   }
}

