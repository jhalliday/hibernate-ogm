/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.cassandra.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.ForeignKey;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.Value;
import org.hibernate.ogm.datastore.spi.BaseSchemaDefiner;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.AssociationKind;
import org.hibernate.ogm.model.key.spi.AssociationType;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.IdSourceKeyMetadata;
import org.hibernate.ogm.type.spi.GridType;
import org.hibernate.ogm.type.spi.TypeTranslator;
import org.hibernate.type.Type;

/**
 * Table and index creation methods.
 *
 * @author Jonathan Halliday
 */
public class CassandraSchemaDefiner extends BaseSchemaDefiner {

	@Override
	public void initializeSchema(SchemaDefinitionContext context) {
		CassandraDatastoreProvider datastoreProvider = (CassandraDatastoreProvider) context.getSessionFactory().getServiceRegistry()
				.getService( DatastoreProvider.class );

		for ( IdSourceKeyMetadata iddSourceKeyMetadata : context.getAllIdSourceKeyMetadata() ) {
			CassandraSequenceHandler sequenceHandler = datastoreProvider.getSequenceHandler();
			sequenceHandler.createSequence( iddSourceKeyMetadata, datastoreProvider );
		}

		Map<String, List<String>> collectionsByOwningTable = new HashMap<>();
		Map<String,Table> childTablesByName = new HashMap<>();

		Set<AssociationKeyMetadata> associationKeyMetadataSet = context.getAllAssociationKeyMetadata();
		for (AssociationKeyMetadata associationKeyMetadata : associationKeyMetadataSet) {
			// BagType (unindexed list) ; ListType (list with @OrderColumn)
			if (associationKeyMetadata.getAssociationKind() == AssociationKind.EMBEDDED_COLLECTION
					&& associationKeyMetadata.getAssociationType() == AssociationType.BAG ) {
				EntityKeyMetadata entityKeyMetadata = associationKeyMetadata.getEntityKeyMetadata();
				String owningTableName = entityKeyMetadata.getTable();
				List<String> childTableList = collectionsByOwningTable.get( owningTableName );
				if ( childTableList == null ) {
					childTableList = new LinkedList<>();
					collectionsByOwningTable.put( owningTableName, childTableList );
				}
				childTableList.add( associationKeyMetadata.getTable() );
				childTablesByName.put( associationKeyMetadata.getTable(), null );
			}
		}

		for ( Namespace namespace : context.getDatabase().getNamespaces() ) {
			for ( Table table : namespace.getTables() ) {
				if ( table.isPhysicalTable() ) {
					if( childTablesByName.containsKey( table.getName() )) {
						childTablesByName.put( table.getName(), table );
					}
				}
			}
		}

		for ( Namespace namespace : context.getDatabase().getNamespaces() ) {
			for ( Table table : namespace.getTables() ) {
				if ( table.isPhysicalTable() ) {
					if( !childTablesByName.containsKey( table.getName() )) {

						List<String> childTableNames = collectionsByOwningTable.get( table.getName() );
						List<Table> childTables = null;
						if(childTableNames != null) {
							childTables = new ArrayList<>( childTableNames.size() );
							for(String name : childTableNames) {
								childTables.add( childTablesByName.get( name ) );
							}
						}

						processTable( context.getSessionFactory(), datastoreProvider, table, childTables );
					}
					datastoreProvider.setTableMetadata( table.getName(), table );
				}
			}
		}
	}

	private void processTable(
			SessionFactoryImplementor sessionFactoryImplementor,
			CassandraDatastoreProvider datastoreProvider,
			Table table,
			List<Table> childTables) {
		TypeTranslator typeTranslator = sessionFactoryImplementor.getServiceRegistry()
				.getService( TypeTranslator.class );

		List<String> primaryKeys = new ArrayList<String>();
		if ( table.hasPrimaryKey() ) {
			for ( Object pkColumn : table.getPrimaryKey().getColumns() ) {
				primaryKeys.add( ((Column) pkColumn).getName() );
			}
		}
		List<String> columnNames = new ArrayList<String>();
		List<String> columnTypes = new ArrayList<String>();

		Iterator<Column> columnIterator = table.getColumnIterator();
		while ( columnIterator.hasNext() ) {
			Column column = columnIterator.next();
			columnNames.add( column.getName() );

			Value value = column.getValue();
			Type type = value.getType();

			if ( type.isAssociationType() ) {
				type = type.getSemiResolvedType( sessionFactoryImplementor );
				if ( type.isComponentType() ) {
					int index = column.getTypeIndex();
					type = ((org.hibernate.type.ComponentType) type).getSubtypes()[index];
				}
			}
			else if ( type.isComponentType() ) {
				int index = column.getTypeIndex();
				type = ((org.hibernate.type.ComponentType) column.getValue().getType()).getSubtypes()[index];
			}

			GridType gridType = typeTranslator.getType( type );
			String cqlType = CassandraTypeMapper.INSTANCE.hibernateToCQL( gridType );
			columnTypes.add( cqlType );
		}

		if(childTables != null) {
			for(Table childTable : childTables) {

				datastoreProvider.setInlinedCollection( childTable.getName(), table );

				List<Column> interestingColumns = new LinkedList<>();
				Iterator<Column> childColumnIterator = childTable.getColumnIterator();
				while(childColumnIterator.hasNext()) {
					interestingColumns.add( childColumnIterator.next() );
				}
				Iterator<ForeignKey> foreignKeyIterator = childTable.getForeignKeyIterator();
				while(foreignKeyIterator.hasNext()) {
					ForeignKey foreignKey = foreignKeyIterator.next();
					if(foreignKey.getReferencedTable().equals( table )) {
						interestingColumns.removeAll( foreignKey.getColumns() );
					}
				}
				if(interestingColumns.size() == 1) {
					// collection of primitives
					Column column = interestingColumns.get( 0 );
					Value value = column.getValue();
					Type type =  value.getType();
					GridType gridType = typeTranslator.getType( type );
					String cqlType = CassandraTypeMapper.INSTANCE.hibernateToCQL( gridType );
					cqlType = "list<"+cqlType+">";
					columnNames.add( column.getName() );
					columnTypes.add( cqlType );
				} else {
					// complex type required
					//throw new RuntimeException( "UDT collection not suported yet" );
				}
			}
		}

		datastoreProvider.createColumnFamilyIfNeeded( table.getName(), primaryKeys, columnNames, columnTypes );
		processIndexes( datastoreProvider, table, primaryKeys );
	}

	private void processIndexes(CassandraDatastoreProvider datastoreProvider, Table table, List<String> primaryKeys) {

		// cassandra won't allow table scanning, so we need to explicitly index for the fk relations:
		Iterator<ForeignKey> fkMappings = table.getForeignKeyIterator();
		while ( fkMappings.hasNext() ) {
			ForeignKey foreignKey = fkMappings.next();

			List<String> fkColumnNames = new ArrayList<String>();

			Iterator<Column> fkColumnIterator = foreignKey.getColumnIterator();
			while ( fkColumnIterator.hasNext() ) {
				Column column = fkColumnIterator.next();
				fkColumnNames.add( column.getName() );
			}

			// cassandra won't allow single index on multiple cols, so index first col only.
			if ( !primaryKeys.contains( fkColumnNames.get( 0 ) ) ) {
				datastoreProvider.createSecondaryIndexIfNeeded( table.getName(), fkColumnNames.get( 0 ) );
			}
		}
	}
}
