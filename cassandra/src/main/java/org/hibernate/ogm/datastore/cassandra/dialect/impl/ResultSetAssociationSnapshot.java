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
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.hibernate.mapping.Table;
import org.hibernate.ogm.datastore.spi.AssociationSnapshot;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.RowKey;

import java.util.*;

/**
 * Created by jhalli on 5/14/14.
 */
public class ResultSetAssociationSnapshot implements AssociationSnapshot {

	private final Table table;
	private Map<RowKey, Row> res = new HashMap<RowKey, Row>();

	public ResultSetAssociationSnapshot(AssociationKey key, ResultSet resultSet, Table tableMetadata) {
		this.table = tableMetadata;

		if(resultSet == null) {
			res = Collections.EMPTY_MAP;
			return;
		}

		for(Row row : resultSet) {
			String[] columnNames = key.getRowKeyColumnNames();
			Object[] columnValues = new Object[columnNames.length];
			for(int i = 0; i < columnNames.length; i++) {
				DataType dataType = row.getColumnDefinitions().getType( i );
				columnValues[i] = dataType.deserialize( row.getBytesUnsafe(columnNames[i]) );
			}
			RowKey rowKey = new RowKey(key.getTable(), columnNames, columnValues);
			res.put(rowKey, row);
		}
	}

	@Override
	public Tuple get(RowKey rowKey) {
		return new Tuple( new ResultSetTupleSnapshot( res.get( rowKey ), table ) );
	}

	@Override
	public boolean containsKey(RowKey rowKey) {
		return res.containsKey( rowKey );
	}

	@Override
	public int size() {
		return res.size();
	}

	@Override
	public Set<RowKey> getRowKeys() {
		return res.keySet();
	}
}
