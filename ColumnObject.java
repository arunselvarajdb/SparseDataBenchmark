
public class ColumnObject {
	String ColumnName;
	String ColumnType;
	int ColumnLength;
	String isNull;
	int group;
	boolean sparse;
	String value;
	
	ColumnObject(String ColumnName,	String ColumnType,	int ColumnLength,	String isNull,	int group,	boolean sparse,String value)
	{
		this.ColumnName=ColumnName;
		this.ColumnType=ColumnType;
		this.ColumnLength= ColumnLength;
		this.isNull=isNull;
		this.group= group;
		this.sparse=sparse;
		this.value=value;
	}
	
}
