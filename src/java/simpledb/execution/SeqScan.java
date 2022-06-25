package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.common.Catalog;
import simpledb.storage.DbFile;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private final Catalog catalog;

    private int tableId;
    private final TransactionId tid;
    private TupleDesc tupleDesc;
    private String tableName;
    private String tableAlias;
    private DbFileIterator iterator;
    private DbFile file;

    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.catalog = Database.getCatalog();

        this.tid = tid;
        this.tableId = tableid;
        this.tupleDesc = changeTupleDesc(catalog.getTupleDesc(tableid), tableAlias);

        this.tableName = catalog.getTableName(tableid);
        this.tableAlias = tableAlias;

        this.file = catalog.getDatabaseFile(tableid);
    }

    private TupleDesc changeTupleDesc(TupleDesc desc, String alias) {
        TupleDesc res = new TupleDesc();
        List<TupleDesc.TDItem> items = new ArrayList<>();
        List<TupleDesc.TDItem> tdItems = desc.getItems();
        for (TupleDesc.TDItem tdItem : tdItems) {
            TupleDesc.TDItem item = new TupleDesc.TDItem(tdItem.fieldType, alias + "." +tdItem.fieldName);
            items.add(item);
        }
        res.setItems(items);
        return res;
    }

    public String getTableName() {
        return this.tableName;
    }

    public String getAlias()
    {
        // some code goes here
        return this.tableAlias;
    }

    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        this.tupleDesc = changeTupleDesc(catalog.getTupleDesc(tableid), tableAlias);
        this.tableName = catalog.getTableName(tableid);
        this.file = catalog.getDatabaseFile(tableid);
        try {
            open();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.iterator = this.file.iterator(this.tid);
        iterator.open();
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (iterator == null) {
            return false;
        }
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (iterator == null) {
            throw new NoSuchElementException("No next tuple");
        }
        Tuple tuple = iterator.next();
        if (tuple == null) {
            throw new NoSuchElementException("No next tuple");
        }
        return tuple;
    }

    public void close() {
        // some code goes here
        iterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        iterator.rewind();
    }
}