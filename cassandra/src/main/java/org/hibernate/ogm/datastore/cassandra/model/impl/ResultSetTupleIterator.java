/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.cassandra.model.impl;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;

import org.hibernate.ogm.dialect.query.spi.ClosableIterator;
import org.hibernate.ogm.model.spi.Tuple;

/**
 * Created by jhalli on 8/21/15.
 */
public class ResultSetTupleIterator implements ClosableIterator<Tuple> {

	private final ResultSet resultSet;
	private final ProtocolVersion protocolVersion;
	private final int max;
	private int count;

	public ResultSetTupleIterator(ResultSet resultSet, ProtocolVersion protocolVersion, int first, int max) {
		this.resultSet = resultSet;
		this.protocolVersion = protocolVersion;
		this.max = max;
		this.count = 0;

		if ( first > 0 ) {
			for ( int i = 0; i < first && hasNext(); i++ ) {
				resultSet.one();
			}
		}

	}

	@Override
	public void close() {
	}

	@Override
	public boolean hasNext() {
		return count < max && !resultSet.isExhausted();
	}

	@Override
	public Tuple next() {
		count++;
		return new Tuple( new ResultSetTupleSnapshot( resultSet.one(), protocolVersion ) );
	}
}
