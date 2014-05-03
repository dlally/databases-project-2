package testPrograms;

import simpledb.index.ehash.ExtensibleHashIndex;
import simpledb.query.Constant;

import java.util.Random;

/**
 * Created by Doug Lally & Nate Miller on 5/3/2014.
 */
public class ExtensibleHashTest {
    static ExtensibleHashIndex ehi;

    public static void main(String[] args){
        ehi = new ExtensibleHashIndex(null, null, null);
        Random r = new Random(System.nanoTime());
        //Populate with 100 random values
        for(int i = 0; i < 100; i++){
            Constant nextVal = new SampleConstant(r.nextInt());
            ehi.insert(nextVal, null);
        }
        //Print it out
        System.out.println(ehi.toString());
    }
}
