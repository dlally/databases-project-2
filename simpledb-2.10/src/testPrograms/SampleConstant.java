package testPrograms;

import simpledb.query.Constant;

/**
 * Created by Doug on 5/3/2014.
 */
public class SampleConstant implements Constant {

    private int i;
    public SampleConstant(int i){
       this.i = i;

    }


    /**
     * Returns the Java object corresponding to this constant.
     *
     * @return the Java value of the constant
     */
    @Override
    public Object asJavaVal() {
        return i;
    }

    @Override
    public int compareTo(Constant o) {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SampleConstant)) return false;

        SampleConstant that = (SampleConstant) o;

        if (i != that.i) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return i;
    }

    @Override
    public String toString() {
        return Integer.toBinaryString(i);
    }
}
