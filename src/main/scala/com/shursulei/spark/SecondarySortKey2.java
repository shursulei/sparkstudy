package com.shursulei.spark;


import java.io.Serializable;
import scala.math.Ordered;

/**
 *  自定义二次排序的Key
 * @author shursulei
 *
 */
public class SecondarySortKey2 implements Ordered<SecondarySortKey2>, Serializable{
	 //需要二次排序的Key
    private int first;
    private int second;

    //二次排序的公开构造器
    public SecondarySortKey2(int first, int second) {
        this.first = first;
        this.second = second;
    }
    public int getFirst() {
        return first;
    }
    public void setFirst(int first) {
        this.first = first;
    }
    public int getSecond() {
        return second;
    }
    public void setSecond(int second) {
        this.second = second;
    }
    public boolean $greater(SecondarySortKey2 other) {
        // TODO Auto-generated method stub
        if(this.first > other.getSecond())
            return true;
        else if(this.first == other.getFirst() && this.second > other.getSecond())
            return true;
        else return false;
    }
    public boolean $greater$eq(SecondarySortKey2 other) {
        // TODO Auto-generated method stub
        if($greater(other))
            return true;
        else if ( this.first == other.getFirst() && this.second == other.second)
            return true;
        else return false;
    }
    public boolean $less(SecondarySortKey2 other) {
        // TODO Auto-generated method stub
        return !$greater$eq(other);
    }
    public boolean $less$eq(SecondarySortKey2 other) {
        // TODO Auto-generated method stub
        return !$greater(other);
    }
    public int compare(SecondarySortKey2 other) {
        // TODO Auto-generated method stub
        if(this.first != other.getFirst())
            return this.first - other.getFirst();
        else return this.second - other.getSecond();
    }
    public int compareTo(SecondarySortKey2 other) {
        // TODO Auto-generated method stub
        if(this.first != other.getFirst())
            return this.first - other.getFirst();
        else return this.second - other.getSecond();
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + first;
        result = prime * result + second;
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SecondarySortKey2 other = (SecondarySortKey2) obj;
        if (first != other.first)
            return false;
        if (second != other.second)
            return false;
        return true;
    }
    /**
     * @param args
     */
    public static void main(String[] args) {

    }

}
