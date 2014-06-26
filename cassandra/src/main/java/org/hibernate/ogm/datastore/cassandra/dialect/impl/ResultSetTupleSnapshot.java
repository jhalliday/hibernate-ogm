/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2012, 2014 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.hibernate.ogm.datastore.cassandra.dialect.impl;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.hibernate.mapping.Table;
import org.hibernate.ogm.datastore.spi.TupleSnapshot;

import java.math.BigDecimal;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by jhalli on 5/14/14.
 */
public class ResultSetTupleSnapshot implements TupleSnapshot {

	private final Row row;
	private Map<String, Integer> columnNames = new HashMap<String, Integer>();
	private Table table;

	public ResultSetTupleSnapshot(Row row, Table tableMetadata) {
		this.table = tableMetadata;
		this.row = row;

		ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
		int count = columnDefinitions.size();
		for (int index = 0; index < count; index++) {
			columnNames.put( columnDefinitions.getName( index ), index );
		}
	}

	@Override
	public Object get(String column) {
		//because, by default, cassandra manages column name in lowercase
		Integer index = columnNames.get( column ); // TODO argss
//		String type = CassandraTypeMapper.INSTANCE.getType( table, column );

		DataType dataType = row.getColumnDefinitions().getType( index );
		Object value = dataType.deserialize( row.getBytesUnsafe(index) );
		return value;


//		if ("blob".equalsIgnoreCase( type )) {
//			return index == null ? null : row.getBytes( index );
//		} else if ("int".equalsIgnoreCase( type )) {
//			return index == null ? null : row.getInt( index );
//		} else if ("byte".equalsIgnoreCase( type )) {
////			return index == null ? null : row.getByte( index );
//			throw new RuntimeException( "implement me!" );
//		} else if ("boolean".equalsIgnoreCase( type )) {
//			return index == null ? null : row.getBool( index );
//		} else if ("float".equalsIgnoreCase( type )) {
//			return index == null ? null : row.getFloat( index );
//		} else if ("bigint".equalsIgnoreCase( type )) {
//			return index == null ? null : row.getString( index );
//		} else if ("calendar_date".equalsIgnoreCase( type )) {
////			return index == null ? null : row.getObject( index );
//			throw new RuntimeException( "implement me!" );
//		} else if ("date".equalsIgnoreCase( type ) || ("timestamp".equalsIgnoreCase( type )) || ("time".equalsIgnoreCase( type ))) {
//			return index == null ? null : row.getString( index );
//		} else if ("decimal".equalsIgnoreCase( type )) {
//			return index == null ? null : BigDecimal.valueOf( row.getDouble( index ) );
//		} else if ("long".equalsIgnoreCase( type )) {
//			return index == null ? null : row.getLong( index );
//		} else {
//			return index == null ? null : row.getString( index );
//		}
	}

	@Override
	public boolean isEmpty() {
		return columnNames.isEmpty();
	}

	@Override
	public Set<String> getColumnNames() {
		return columnNames.keySet();
	}
}
