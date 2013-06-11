package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their associated schemas. For now, this is a stub
 * catalog that must be populated with tables by a user program before it can be used -- eventually, this should be
 * converted to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {
    private final Map<String, Integer> names;
    private final Map<Integer, Table> tables;

    /**
     * Constructor. Creates a new, empty catalog.
     */
    public Catalog() {
        names = new ConcurrentHashMap<String, Integer>();
        tables = new ConcurrentHashMap<Integer, Table>();
    }

    /**
     * Add a new table to the catalog. This table has tuples formatted using the specified TupleDesc and its contents
     * are stored in the specified DbFile.
     * 
     * @param file
     *            the contents of the table to add; file.getId() is the identfier of this file/tupledesc param for the
     *            calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, UUID.randomUUID().toString());
    }

    /**
     * Add a new table to the catalog. This table's contents are stored in the specified DbFile.
     * 
     * @param file
     *            the contents of the table to add; file.getId() is the identfier of this file/tupledesc param for the
     *            calls getTupleDesc and getFile
     * @param name
     *            the name of the table -- may be an empty string. May not be null. If a name conflict exists, use the
     *            last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name) {
        addTable(file, name, null);
    }

    /**
     * Add a new table to the catalog. This table's contents are stored in the specified DbFile.
     * 
     * @param file
     *            the contents of the table to add; file.getId() is the identfier of this file/tupledesc param for the
     *            calls getTupleDesc and getFile
     * @param name
     *            the name of the table -- may be an empty string. May not be null. If a name conflict exists, use the
     *            last table to be added as the table for a given name.
     * @param pkeyField
     *            the name of the primary key field
     * 
     * @throws IllegalArgumentException
     *             if file or name is null.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        if (file == null || name == null) {
            throw new IllegalArgumentException();
        }

        names.put(name, file.getId());

        Table table = new Table(file, name, pkeyField);
        tables.put(file.getId(), table);
    }

    /**
     * Returns the id of the table with a specified name,
     * 
     * @param name
     *            The name of the table to look up.
     * 
     * @return the id of the table.
     * 
     * @throws NoSuchElementException
     *             if the table doesn't exist
     */
    public int getTableId(String name) {
        if (name == null) {
            throw new NoSuchElementException();
        }

        Integer id = names.get(name);

        if (id == null) {
            throw new NoSuchElementException();
        }

        return id;
    }

    /**
     * Returns the Table with the given tableid.
     * 
     * @param tableid
     *            The tableid to get the Table for.
     * 
     * @return the Table with the given tableid.
     * 
     * @throws NoSuchElementException
     *             if the table does not exist.
     */
    private Table getTable(int tableid) {
        Table table = tables.get(tableid);

        if (table == null) {
            throw new NoSuchElementException();
        }

        return table;
    }

    /**
     * Returns the DbFile that can be used to read the contents of the specified table.
     * 
     * @param tableid
     *            The id of the table, as specified by the DbFile.getId() function passed to addTable
     * 
     * @return the DbFile with the given tableid.
     * 
     * @throws NoSuchElementException
     *             if the table doesn't exist.
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        return getTable(tableid).file;
    }

    /**
     * Returns the name of the field that is the primary key for the table, or null if none given.
     * 
     * @param tableid
     *            The id of the table to get the primary key for.
     * 
     * @return the name of the primary key or null if none.
     * 
     * @throws NoSuchElementException
     *             if the table doesn't exist.
     */
    public String getPrimaryKey(int tableid) {
        return getTable(tableid).pkey;
    }

    /**
     * Returns the name of the table represented by tableid.
     * 
     * @param tableid
     *            The tableid to get the name for.
     * 
     * @return The name of the table.
     * 
     * @throws NoSuchElementException
     *             if the tableid does not exist.
     */
    public String getTableName(int tableid) {
        return getTable(tableid).name;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * 
     * @param tableid
     *            The id of the table, as specified by the DbFile.getId() function passed to addTable
     * 
     * @return the schema of the table.
     * 
     * @throws NoSuchElementException
     *             if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) {
        return getDbFile(tableid).getTupleDesc();
    }

    /**
     * @return an iterator over the table ids.
     */
    public Iterator<Integer> tableIdIterator() {
        return tables.keySet().iterator();
    }

    /** Delete all tables from the catalog */
    public void clear() {
        names.clear();
        tables.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * 
     * @param catalogFile
     *            The file name of the catalog to load.
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                // assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                // System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int")) {
                        types.add(Type.INT_TYPE);
                    } else if (els2[1].trim().toLowerCase().equals("string")) {
                        types.add(Type.STRING_TYPE);
                    } else {
                        System.out.println("Unknown type " + els2[1]);
                        br.close();
                        br = null;
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk")) {
                            primaryKey = els2[0].trim();
                        } else {
                            System.out.println("Unknown annotation " + els2[2]);
                            br.close();
                            br = null;
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    private class Table {
        public final DbFile file;
        public final String pkey;
        public final String name;

        public Table(DbFile file, String name, String pkey) {
            this.file = file;
            this.name = name;
            this.pkey = pkey;
        }
    }
}
