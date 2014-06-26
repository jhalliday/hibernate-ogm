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
package org.hibernate.ogm.datastore.cassandra;

import com.datastax.driver.core.*;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.*;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Table;
import org.hibernate.ogm.datastore.cassandra.dialect.impl.*;
import org.hibernate.ogm.datastore.cassandra.impl.CassandraDatastoreProvider;
import org.hibernate.ogm.datastore.cassandra.logging.impl.Log;
import org.hibernate.ogm.datastore.cassandra.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.map.impl.MapAssociationSnapshot;
import org.hibernate.ogm.datastore.spi.*;
import org.hibernate.ogm.dialect.BatchableGridDialect;
import org.hibernate.ogm.dialect.batch.*;
import org.hibernate.ogm.exception.NotSupportedException;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.EntityKeyMetadata;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.loader.nativeloader.BackendCustomQuery;
import org.hibernate.ogm.massindex.batchindexing.Consumer;
import org.hibernate.ogm.query.spi.ParameterMetadataBuilder;
import org.hibernate.ogm.type.*;
import org.hibernate.ogm.util.ClosableIterator;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.Type;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by jhalli on 5/9/14.
 */
public class CassandraDialect implements BatchableGridDialect {

	private final CassandraDatastoreProvider provider;

	private static final Log log = LoggerFactory.getLogger();

	public CassandraDialect(CassandraDatastoreProvider provider) {
		this.provider = provider;
	}

	@Override
	public LockingStrategy getLockingStrategy(Lockable lockable, LockMode lockMode) {
		//Cassandra essentially has no workable lock strategy unless you use external tools like
		// ZooKeeper or any kind of lock keeper
		// FIXME find a way to reflect that in the implementation
		return null;
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
		String table = key.getTable();

		//TODO : fix me : ok just for simple pk
		String idColumnName = key.getColumnNames()[0];

		StringBuilder query = new StringBuilder( "SELECT * " )
				.append( "FROM " )
				.append( "\"" )
				.append( table )
				.append( "\"" )
				.append( " WHERE " )
				.append( "\"" )
				.append( idColumnName )
				.append( "\"" )
				.append( "=?" );

		ResultSet resultSet;
		boolean next;
		PreparedStatement preparedStatement = provider.getSession().prepare( query.toString() ); // TODO NPE
		BoundStatement boundStatement = preparedStatement.bind( key.getColumnValues()[0] );
		resultSet = provider.getSession().execute( boundStatement );

		if (resultSet.isExhausted()) {
			return null;
		}

		Row row = resultSet.one();
		Tuple tuple = new Tuple( new ResultSetTupleSnapshot( row, this.provider.getMetaDataCache().get( table ) ) );
		return tuple;
	}

	@Override
	public Tuple createTuple(EntityKey key, TupleContext tupleContext) {
		Map<String, Object> toSave = new HashMap<String, Object>();
		toSave.put( key.getColumnNames()[0], key.getColumnValues()[0] );
		return new Tuple( new MapBasedTupleSnapshot( toSave ) );
	}

	@Override
	public void updateTuple(Tuple tuple, EntityKey key, TupleContext tupleContext) {
		String table = key.getTable();

		//TODO : fix me : ok just for simple pk
		String idColumnName = key.getColumnNames()[0];
		StringBuilder query = new StringBuilder();

		List<TupleOperation> updateOps = new ArrayList<TupleOperation>( tuple.getOperations().size() );
		List<TupleOperation> deleteOps = new ArrayList<TupleOperation>( tuple.getOperations().size() );


		for (TupleOperation op : tuple.getOperations()) {
			switch (op.getType()) {
				case PUT:
					updateOps.add( op );
					break;
				case REMOVE:
				case PUT_NULL:
					deleteOps.add( op );
					break;
				default:
					throw new HibernateException( "TupleOperation not supported: " + op.getType() );
			}
			if (deleteOps.size() > 0) {
				query.append( "DELETE " )
						// column
						//TODO Finish this column
						.append( " FROM " )
						.append( "\"" )
						.append( table )
						.append( "\"" )
						.append( " WHERE " )
						.append( "\"" )
						.append( idColumnName )
						.append( "\"" )
						.append( "=?;" );
			}
		}

		StringBuilder keyList = new StringBuilder();
		StringBuilder valueList = new StringBuilder();

		String prefix = "";
		//cassandra can not insert just the pk and need at least one column: http://cassandra.apache.org/doc/cql/CQL.html#INSERT
		boolean containsKey = false;
		for (TupleOperation op : updateOps) {
			keyList.append( prefix );
			valueList.append( prefix );
			prefix = ",";
			keyList.append( "\"" ).append( op.getColumn() ).append( "\"" );
			if (op.getColumn().equals( idColumnName )) {
				containsKey = true;
			}
			valueList.append( "'" ).append( op.getValue() ).append( "'" );
		}

		String queryQuestionMark = generateQueryQuestionMark( updateOps.size() );

		//check if keyList contains the key. If not, add it
		if (!containsKey) {
			keyList.append( "," ).append( "\"" ).append( idColumnName ).append( "\"" );
			queryQuestionMark += ",?";
		}

		//TODO: fixme: because it is not possible to insert a row without another column
		if (!keyList.toString().contains( "," )) {
			Table tableMetadata = provider.getMetaDataCache().get( table );
			Iterator columnIterator = tableMetadata.getColumnIterator();
			while (columnIterator.hasNext()) {
				Column column = (Column) columnIterator.next();
				if (!keyList.toString().contains( column.getName() ) && !"dtype".equalsIgnoreCase( column.getName() )) {
					keyList.append( "," + column.getName() );
					queryQuestionMark += ", 'null'";
					break;
				}
			}
		}

		query.append( "INSERT INTO " )
				.append( "\"" )
				.append( table )
				.append( "\"" )
				.append( "(" )
				.append( keyList )
				.append( ") VALUES(" )
				.append( queryQuestionMark )
				.append( ");" );


		PreparedStatement preparedStatement = provider.getSession().prepare( query.toString() );
		BoundStatement boundStatement = new BoundStatement( preparedStatement );
		for (int i = 0; i < updateOps.size(); i++) {

			TupleOperation tupleOperation = updateOps.get( i );
			String type = CassandraTypeMapper.INSTANCE.getType( this.provider, table, tupleOperation.getColumn() );

			DataType dataType = preparedStatement.getVariables().getType( i );
			boundStatement.setBytesUnsafe( i, dataType.serialize( tupleOperation.getValue() ) );


//			if ("blob".equalsIgnoreCase( type )) {
//				ByteBuffer byteBuffer = ByteBuffer.wrap( (byte[]) updateOps.get( i ).getValue() );
//				boundStatement.setBytes( i, byteBuffer );
//			} else if ("int".equalsIgnoreCase( type )) {
//				boundStatement.setInt( i, Integer.valueOf( tupleOperation.getValue().toString() ) );
//			} else if ("boolean".equalsIgnoreCase( type )) {
//				boundStatement.setBool( i, Boolean.valueOf( tupleOperation.getValue().toString() ) );
//			} else if ("float".equalsIgnoreCase( type )) {
//				boundStatement.setFloat( i, Float.valueOf( tupleOperation.getValue().toString() ) );
//			} else {
//				boundStatement.setString( i, tupleOperation.getValue().toString() );
//			}
		}
		if (!containsKey) {
			boundStatement.setString( updateOps.size(), key.getColumnValues()[0].toString() );
		}

		ResultSet resultSet = provider.getSession().execute( boundStatement );
	}


	// TODO deDup w/ getTuple
	@Override
	public void removeTuple(EntityKey key, TupleContext tupleContext) {
		String table = key.getTable();

		//TODO : fix me : ok just for simple pk
		String idColumnName = key.getColumnNames()[0];

		StringBuilder query = new StringBuilder( "DELETE " )
				.append( "FROM " )
				.append( "\"" )
				.append( table )
				.append( "\"" )
				.append( " WHERE " )
				.append( "\"" )
				.append( idColumnName )
				.append( "\"" )
				.append( "=?" );

		PreparedStatement preparedStatement = provider.getSession().prepare( query.toString() );
		BoundStatement boundStatement = preparedStatement.bind( key.getColumnValues()[0].toString() );
		ResultSet resultSet = provider.getSession().execute( boundStatement );
	}

	@Override
	public Association getAssociation(AssociationKey key, AssociationContext associationContext) {
		String table = key.getTable();

		// TODO dedup with getTuple

		//TODO : fix me : ok just for simple pk
		String idColumnName = key.getColumnNames()[0];

		StringBuilder query = new StringBuilder( "SELECT * " )
				.append( "FROM " )
				.append( "\"" )
				.append( table )
				.append( "\"" )
				.append( " WHERE " )
				.append( "\"" )
				.append( idColumnName )
				.append( "\"" )
				.append( "=?" );

		ResultSet resultSet;
		boolean next;
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = provider.getSession().prepare( query.toString() );
		} catch(InvalidQueryException e) {
			log.error( query.toString(), e );
			throw e;
		}
		BoundStatement boundStatement = preparedStatement.bind( key.getColumnValues()[0].toString() );
		resultSet = provider.getSession().execute( boundStatement );

		if (resultSet.isExhausted()) {
			return null;
		}

		Association association = new Association(
				new ResultSetAssociationSnapshot(
						key,
						resultSet,
						provider.getMetaDataCache()
								.get( table )
				)
		);

		return association;
	}

	@Override
	public Association createAssociation(AssociationKey key, AssociationContext associationContext) {
		Table tableMetadata = provider.getMetaDataCache().get( key.getTable() );
		return new Association( new ResultSetAssociationSnapshot(key, null, tableMetadata) );
	}

	@Override
	public void updateAssociation(Association association, AssociationKey key, AssociationContext associationContext) {

		// 'key' has only the parent entity key. From the db's point of view the child key is also part of the table's pk,
		// so we need to go fishing for that in the metadata...

		// key.getColumnNames() -> 1: UserWithNotNullCollection_id.
		// so, how to id the remaining part of the table PK: nick.
		Table tableMetadata = provider.getMetaDataCache().get( key.getTable() );
		Set<String> keyColumnNames = new HashSet<String>();
		for(Object columnObject : tableMetadata.getPrimaryKey().getColumns()) {
			Column column = (Column) columnObject;
			keyColumnNames.add( column.getName() );
		}

		tableMetadata.getPrimaryKey().getColumns(); // 2: UserWithNotNullCollection_id, nick

		List<AssociationOperation> updateOps = new ArrayList<AssociationOperation>( association.getOperations().size() );
		List<AssociationOperation> deleteOps = new ArrayList<AssociationOperation>( association.getOperations().size() );

		for(AssociationOperation op : association.getOperations()) {
			switch (op.getType()) {
				case CLEAR:
					// TODO
					break;
				case PUT:
					updateOps.add( op );
					break;
				case REMOVE:
				case PUT_NULL:
					deleteOps.add( op ); // TODO process delete ops
					break;
				default:
					throw new HibernateException( "AssociationOperation not supported: " + op.getType() );
			}
		}

		// can't update pk-only tables, use more general insert instead?

		BatchStatement batchStatement = new BatchStatement();

		for(AssociationOperation op : updateOps) {
			Tuple value = op.getValue();
			Insert insert = insertInto( "\"" + key.getTable() + "\"" );
			for(String columnName : value.getColumnNames()) {
					insert.value( "\""+ columnName + "\"" , QueryBuilder.bindMarker(columnName) );
			}

			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = provider.getSession().prepare( insert.getQueryString() );
			} catch(RuntimeException e) {
				System.out.println("stmt: "+insert.getQueryString());
				throw e;
			}
			BoundStatement boundStatement = preparedStatement.bind();

			for(String columnName : value.getColumnNames()) {
				Object v = value.get( columnName );

				DataType dataType = preparedStatement.getVariables().getType( columnName );
				boundStatement.setBytesUnsafe( columnName, dataType.serialize( v ) );
			}

			batchStatement.add( boundStatement );
		}

		for(AssociationOperation op : deleteOps) {
			RowKey value = op.getKey();
			Delete delete = delete().from( "\"" + key.getTable() + "\"" );
			for(String columnName : value.getColumnNames()) {
				delete.where( eq( "\"" + columnName + "\"", QueryBuilder.bindMarker(columnName) ) );
			}

			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = provider.getSession().prepare(  delete.getQueryString() );
			} catch(RuntimeException e) {
				System.out.println("stmt: "+delete.getQueryString());
				throw e;
			}
			BoundStatement boundStatement = preparedStatement.bind();

			for(int i = 0; i < value.getColumnNames().length; i++) {
				String columnName = value.getColumnNames()[i];
				Object v = value.getColumnValues()[i];

				DataType dataType = preparedStatement.getVariables().getType( columnName );
				boundStatement.setBytesUnsafe(  columnName, dataType.serialize( v ) );
			}

			batchStatement.add( boundStatement );
		}

		provider.getSession().execute( batchStatement );
	}

	private String generateQueryQuestionMark(int nbVariable) {
		StringBuilder sb = new StringBuilder();
		String prefix = "";
		for (int i = 0; i < nbVariable; i++) {
			sb.append( prefix );
			prefix = ",";
			sb.append( "?" );
		}
		return sb.toString();
	}

	@Override
	public void removeAssociation(AssociationKey key, AssociationContext associationContext) {
		//throw new NotSupportedException( "OGM-122", "add association support" );

		String table = key.getTable();

		//TODO : fix me : ok just for simple pk
		String idColumnName = key.getColumnNames()[0];

		StringBuilder query = new StringBuilder( "DELETE " )
				.append( "FROM " )
				.append( "\"" )
				.append( table )
				.append( "\"" )
				.append( " WHERE " )
				.append( "\"" )
				.append( idColumnName )
				.append( "\"" )
				.append( "=?" );

		PreparedStatement preparedStatement = provider.getSession().prepare( query.toString() );
		BoundStatement boundStatement = preparedStatement.bind( key.getColumnValues()[0].toString() );
		ResultSet resultSet = provider.getSession().execute( boundStatement );

	}

	@Override
	public Tuple createTupleAssociation(AssociationKey associationKey, RowKey rowKey) {
		return new Tuple();

//		Map<String, Object> toSave = new HashMap<String, Object>();
//		toSave.put( rowKey.getColumnValues()[0].toString(), null );
//		return new Tuple( new MapBasedTupleSnapshot( toSave ) ); // TODO fix me
	}

	private Object nextValueSelect(String tableName, String sequenceName) {

		Statement select = select().column( "sequence_value" ).from( "\"" + tableName + "\"" )
				.where( eq("sequence_name", QueryBuilder.bindMarker()) );

		PreparedStatement preparedStatement = provider.getSession().prepare( select.toString() );
		BoundStatement boundStatement = preparedStatement.bind(sequenceName);
		ResultSet resultSet = provider.getSession().execute( boundStatement );
		if(resultSet.isExhausted()) {
			return null;
		} else {
			return resultSet.one().getLong( 0 );
		}
	}

	private Object nextValueInsert(String tableName, String sequenceName, Long value) {

		Insert insert = insertInto( "\"" + tableName + "\"" )
				.value( "sequence_name", QueryBuilder.bindMarker("sequence_name") )
				.value( "sequence_value", QueryBuilder.bindMarker("sequence_value") )
				.ifNotExists();

		PreparedStatement preparedStatement = provider.getSession().prepare( insert.toString() );
		BoundStatement boundStatement = preparedStatement.bind();
		boundStatement.setString( "sequence_name", sequenceName );
		boundStatement.setLong( "sequence_value", value );
		provider.getSession().execute( boundStatement );

		return nextValueSelect( tableName, sequenceName );
	}

	private boolean nextValueUpdate(String tableName, String sequenceName, Long oldValue, Long newValue) {

		Statement update = update( "\"" + tableName + "\"" )
				.with( set("sequence_value", QueryBuilder.bindMarker("sequence_value_new")) )
				.where( eq("sequence_name", QueryBuilder.bindMarker("sequence_name")) )
				.onlyIf( eq("sequence_value", QueryBuilder.bindMarker("sequence_value_old")) );

		PreparedStatement preparedStatement = provider.getSession().prepare( update.toString() );
		BoundStatement boundStatement = preparedStatement.bind();
		boundStatement.setString( "sequence_name", sequenceName );
		boundStatement.setLong( "sequence_value_new", newValue );
		boundStatement.setLong( "sequence_value_old", oldValue );
		ResultSet resultSet = provider.getSession().execute( boundStatement );
		return resultSet.one().getBool( 0 );
	}

	@Override
	public void nextValue(RowKey key, IntegralDataTypeHolder value, int increment, int initialValue) {
		// TODO wire seq table init via SessionFactoryObserver per neoj4. register Integrator in META-INF/services
		System.out.println("implement me!");
		// RowKey: table=hibernate_sequences, columnname=sequence_name, colvalue=IndexedLabel
		// ROwKey: table=sequences columnname=key, colvlaue=music






		boolean done = false;
		do {
			Object valueFromDb = nextValueSelect( key.getTable(), key.getColumnValues()[0].toString() );

			if ( valueFromDb == null ) {
				//if not there, insert initial value
				value.initialize( initialValue );
				valueFromDb = new Long( value.makeValue().longValue() );
				final Object oldValue = nextValueInsert( key.getTable(), key.getColumnValues()[0].toString(), (Long)valueFromDb );
				//check in case somebody has inserted it behind our back
				if ( oldValue != null ) {
					value.initialize( ( (Number) oldValue ).longValue() );
					valueFromDb = oldValue;
				}
			}
			else {
				//read the value from the table
				value.initialize( ( (Number) valueFromDb ).longValue() );
			}

			//update value
			final IntegralDataTypeHolder updateValue = value.copy();
			//increment value
			updateValue.add( increment );
			final Object newValueFromDb = updateValue.makeValue();
			done = nextValueUpdate( key.getTable(), key.getColumnValues()[0].toString(), (Long)valueFromDb, (Long)newValueFromDb );
		}
		while ( !done );

	}

	@Override
	public GridType overrideType(Type type) {
		if (type == StandardBasicTypes.INTEGER) {
			return IntegerType.INSTANCE;
		}
		if (type == StandardBasicTypes.BIG_INTEGER) {
			return BigIntegerType.INSTANCE;
		}
		if (type == StandardBasicTypes.BIG_DECIMAL) {
//			return StringBigDecimal.INSTANCE;
			throw new RuntimeException( "implement me!" );
		}
		if (type == StandardBasicTypes.CALENDAR_DATE || type == StandardBasicTypes.CALENDAR) {
			return CalendarType.INSTANCE;
		}
		if (type == StandardBasicTypes.TIMESTAMP) {
			return DateType.INSTANCE;
		}
		if (type == StandardBasicTypes.TIME) {
			return DateType.INSTANCE;
		}
		if (type == StandardBasicTypes.DATE) {
			return DateType.INSTANCE;
		}
		//TODO : fix me
//		if (type.getName().equals( "org.hibernate.type.EnumType" ) ) {
//		}
		return null;
	}

	//////////


	// TODO real batching
	@Override
	public void executeBatch(OperationsQueue queue) {
		if ( !queue.isClosed() ) {
			Operation operation = queue.poll();
//			Map<DBCollection, BatchInsertionTask> inserts = new HashMap<DBCollection, BatchInsertionTask>();
			while ( operation != null ) {
				if ( operation instanceof UpdateTupleOperation) {
					UpdateTupleOperation update = (UpdateTupleOperation) operation;
					updateTuple(  update.getTuple(), update.getEntityKey(), update.getTupleContext() );
//					executeBatchUpdate( inserts, update );
				}
				else if ( operation instanceof RemoveTupleOperation) {
					RemoveTupleOperation tupleOp = (RemoveTupleOperation) operation;
					removeTuple(  tupleOp.getEntityKey(), tupleOp.getTupleContext() );
//					executeBatchRemove( inserts, tupleOp );
				}
				else if ( operation instanceof UpdateAssociationOperation) {
					UpdateAssociationOperation update = (UpdateAssociationOperation) operation;
					updateAssociation( update.getAssociation(), update.getAssociationKey(), update.getContext() );
				}
				else if ( operation instanceof RemoveAssociationOperation ) {
					RemoveAssociationOperation remove = (RemoveAssociationOperation) operation;
					removeAssociation( remove.getAssociationKey(), remove.getContext() );
				}
				else {
					throw new UnsupportedOperationException( "Operation not supported on MongoDB: " + operation.getClass().getName() );
				}
				operation = queue.poll();
			}
//			flushInserts( inserts );
			queue.close();
		}

	}


	@Override
	public boolean isStoredInEntityStructure(AssociationKey associationKey, AssociationContext associationContext) {
		return false;
	}

	@Override
	public void forEachTuple(Consumer consumer, EntityKeyMetadata... entityKeyMetadatas) {

	}

	@Override
	public ClosableIterator<Tuple> executeBackendQuery(BackendCustomQuery customQuery, QueryParameters queryParameters, EntityKeyMetadata[] metadatas) {
		return null;
	}

	@Override
	public ParameterMetadataBuilder getParameterMetadataBuilder() {
		return null;
	}

}
