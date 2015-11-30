package simpledb;

public class Table {

    private final String name;
    private final DbFile file;
    private final String pkeyFieldName;
    
    public Table(String name, DbFile file, String pkeyFieldName) {
        this.name = name;
        this.file = file;
        this.pkeyFieldName = pkeyFieldName;
    }

    public String getName() {
        return name;
    }

    public DbFile getFile() {
        return file;
    }

    public String getPkeyFieldName() {
        return pkeyFieldName;
    }
    
    public int getId() {
        return file.getId();
    }
    
    public TupleDesc getTupleDesc() {
        return file.getTupleDesc();
    }
}
