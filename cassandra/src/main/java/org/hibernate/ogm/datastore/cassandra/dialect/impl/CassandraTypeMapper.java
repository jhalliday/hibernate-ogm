package org.hibernate.ogm.datastore.cassandra.dialect.impl;

import org.hibernate.mapping.Column;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;
import org.hibernate.ogm.datastore.cassandra.impl.CassandraDatastoreProvider;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by jhalli on 5/14/14.
 */
public enum CassandraTypeMapper {

	INSTANCE;

	public static final String UNKNOWN_TYPE = "unknown";

	private Map<String, String> mapper = new HashMap<String, String>(  );
	{
		// hibernate -> cql3 type mappings
		// see also: http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/cql_data_types_c.html
		mapper.put( "materialized_blob", "blob" );
		mapper.put( "int", "int" );
		mapper.put( "date", "date" );
		mapper.put( "calendar", "calendar_date" );
		mapper.put( "calendar_date", "calendar_date" );
		mapper.put( "java.lang.Byte", "byte" );
		mapper.put( "java.lang.Boolean", "boolean" );
		mapper.put( "java.util.UUID", "uuid" );
		mapper.put( "java.math.BigDecimal", "decimal" );
		mapper.put( "java.lang.Integer", "int" );
		mapper.put( "integer", "int" );
		mapper.put( "java.math.BigInteger", "bigint" );
		mapper.put( "java.lang.Long", "bigint" );
		mapper.put( "java.lang.Float", "float" );
		mapper.put( "java.net.URL", "varchar" );

		mapper.put( "double", "double" );
		mapper.put( "java.lang.String", "text" );
	}

	public String hibernateToCQL(String columnType) {
		String cqlType = CassandraTypeMapper.INSTANCE.mapper.get( columnType );

		if(cqlType == null) {
			cqlType = "text";
		}

//		String tmpInnerType = (innerType == null || innerType.equals( "byte" ) || innerType
//				.equals( "uuid" ) || innerType.equals( "calendar_date" ) || innerType.equals( "date" ) || innerType
//				.equals( "long" ) || innerType.equals( "decimal" )) ? "varchar" : innerType;

		return cqlType;
	}

	public String getType(CassandraDatastoreProvider provider, String tableName, String columnName) {
		Table table = provider.getMetaDataCache().get( tableName );
		Iterator<Column> columnsIt = (Iterator<Column>) table.getColumnIterator();

		while (columnsIt.hasNext()) {
			Column column = columnsIt.next();
			if (columnName.equalsIgnoreCase( column.getName() ) ) {
				String columnType = ((SimpleValue) column.getValue()).getTypeName();
				String innerType = CassandraTypeMapper.INSTANCE.mapper.get( columnType );
				return (innerType == null) ? "varchar" : innerType;

			}
		}
		return CassandraTypeMapper.UNKNOWN_TYPE;
	}

	public String getType(Table table, String columnName) {
		Iterator<Column> columnsIt = (Iterator<Column>) table.getColumnIterator();

		while (columnsIt.hasNext()) {
			Column column = columnsIt.next();
			if (columnName.equalsIgnoreCase( column.getName() ) ) {
				String columnType = ((SimpleValue) column.getValue()).getTypeName();
				String innerType = CassandraTypeMapper.INSTANCE.mapper.get( columnType );

				return (innerType == null) ? "varchar" : innerType;
			}
		}
		return CassandraTypeMapper.UNKNOWN_TYPE;
	}
}
