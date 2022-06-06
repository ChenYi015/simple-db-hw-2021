package simpledb.storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PageId pageId;

    private final int tupleNumber;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pageId
     *            the page id of the page on which the tuple resides
     * @param tupleNumber
     *            the tuple number within the page.
     */
    public RecordId(PageId pageId, int tupleNumber) {
        // some code goes here
        this.pageId = pageId;
        this.tupleNumber = tupleNumber;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return tupleNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (! (o instanceof RecordId)) {
            return false;
        }
        RecordId recordId = (RecordId) o;
        return pageId.equals(recordId.getPageId()) && tupleNumber == recordId.getTupleNumber();
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        return Objects.hash(pageId, tupleNumber);
    }

}
