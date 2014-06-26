package org.hibernate.ogm.datastore.cassandra.dialect.impl;

import org.hibernate.ogm.datastore.cassandra.impl.CassandraDatastoreProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by jhalli on 6/4/14.
 */
public class CassandraSequenceGenerator {

	private final CassandraDatastoreProvider datastoreProvider;

	public CassandraSequenceGenerator(CassandraDatastoreProvider datastoreProvider) {
		this.datastoreProvider = datastoreProvider;
	}

	public void createUniqueConstraint(Set<String> generatorsKey) {

		for(String generatorTableName : generatorsKey) {
			// ogm wiring doesn't allow us to get at the real col names :-(
			List<String> primaryKeyName = new ArrayList<String>( 1 );
			primaryKeyName.add( "sequence_name" );
			List<String> columnNames = new ArrayList<String>( 2 );
			columnNames.add( "sequence_name" );
			columnNames.add( "sequence_value" );
			List<String> columnTypes = new ArrayList<String>( 2 );
			columnTypes.add( "varchar" );
			columnTypes.add( "java.lang.Long" );
			datastoreProvider.createColumnFamilyIfNeeded( generatorTableName, primaryKeyName, columnNames, columnTypes );
		}
	}
}
