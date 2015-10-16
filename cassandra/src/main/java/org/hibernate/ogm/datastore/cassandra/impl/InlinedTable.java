/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.cassandra.impl;

import com.datastax.driver.core.UserType;

import org.hibernate.mapping.Table;

/**
 * Mapping data for embedded object collections that are modelled at cassandra collections of UDTs.
 *
 * @author Jonathan Halliday
 */
public class InlinedTable {

	private final Table table;
	private final String udtColumnName;
	private final UserType userType;

	public InlinedTable(Table table) {
		this(table, null, null);
	}

	public InlinedTable(Table table, UserType userType, String udtColumnName) {
		this.table = table;
		this.userType = userType;
		this.udtColumnName = udtColumnName;
	}

	public Table getTable() {
		return table;
	}

	public UserType getUserType() {
		return userType;
	}

	public String getUdtColumnName() {
		return udtColumnName;
	}
}
