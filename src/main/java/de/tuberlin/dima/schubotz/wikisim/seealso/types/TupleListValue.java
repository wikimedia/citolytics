package de.tuberlin.dima.schubotz.wikisim.seealso.types;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by malteschwarzer on 05.03.15.
 */
public class TupleListValue implements Collection<Tuple2<String, Integer>> {
    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator<Tuple2<String, Integer>> iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public boolean add(Tuple2<String, Integer> stringIntegerTuple2) {

        return false;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends Tuple2<String, Integer>> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {

    }
}
