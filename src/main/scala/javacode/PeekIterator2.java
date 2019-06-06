package javacode;

import java.util.Iterator ;
import java.util.NoSuchElementException ;
import java.util.Queue ;

/** PeekIterator - is one slot ahead from the wrapped iterator */
public class PeekIterator2<T> implements Iterator<T>
{
    private final Iterator<T> iter ;
    private boolean finished = false ;
    // Slot always full when iterator active.  Null is a valid element.
    private T slot ;

    public static <T> org.apache.jena.atlas.iterator.PeekIterator<T> create(org.apache.jena.atlas.iterator.PeekIterator<T> iter) { return iter ; }
    public static <T> org.apache.jena.atlas.iterator.PeekIterator<T> create(Iterator<T> iter)
    {
        if ( iter instanceof org.apache.jena.atlas.iterator.PeekIterator<?>)
            return (org.apache.jena.atlas.iterator.PeekIterator<T>)iter ;
        return new org.apache.jena.atlas.iterator.PeekIterator<>(iter) ;
    }

    public PeekIterator2(Iterator<T> iter)
    {
        this.iter = iter ;
        fill() ;
    }

    private void fill()
    {
        if ( finished ) return ;
        if ( iter.hasNext() )
            slot = iter.next();
        else
        {
            finished = true ;
            slot = null ;
        }
    }

    @Override
    public boolean hasNext()
    {
        if ( finished )
            return false ;
        return true ;
    }

    /** Peek the next element or return null
     *  @see Queue#peek
     */
    public T peek()
    {
        if ( finished )
            return null  ;
        return slot ;
    }

    /** Peek the next element or throw  NoSuchElementException */
    public T element()
    {
        if ( finished )
            throw new NoSuchElementException() ;
        return slot ;
    }

    @Override
    public T next()
    {
        if ( finished )
            throw new NoSuchElementException() ;
        T x = slot ;
        // Move on now so the slot is loaded for peek.
        fill() ;
        return x ;
    }

    @Override
    public void remove()
    { throw new UnsupportedOperationException() ; }

}

