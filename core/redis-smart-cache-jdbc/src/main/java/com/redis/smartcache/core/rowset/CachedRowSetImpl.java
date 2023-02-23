/*
 * Copyright (c) 2003, 2021, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package com.redis.smartcache.core.rowset;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import javax.sql.RowSet;
import javax.sql.RowSetEvent;
import javax.sql.RowSetInternal;
import javax.sql.RowSetMetaData;
import javax.sql.rowset.BaseRowSet;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetWarning;
import javax.sql.rowset.serial.SQLInputImpl;
import javax.sql.rowset.serial.SerialArray;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialRef;
import javax.sql.rowset.serial.SerialStruct;
import javax.sql.rowset.spi.SyncProvider;
import javax.sql.rowset.spi.SyncProviderException;

/**
 * The standard implementation of the <code>CachedRowSet</code> interface.
 *
 * See interface definition for full behavior and implementation requirements.
 * This reference implementation has made provision for a one-to-one write back
 * facility and it is curremtly be possible to change the peristence provider
 * during the life-time of any CachedRowSetImpl.
 *
 * @author Jonathan Bruce, Amit Handa
 */

public class CachedRowSetImpl extends BaseRowSet implements RowSet, RowSetInternal, Serializable, CachedRowSet {

	private static final String MESSAGE_CLONE_FAIL = "Clone failed: {0}";
	private static final String MESSAGE_DATA_TYPE_MISMATCH = "Data Type Mismatch";
	private static final String MESSAGE_INVALID_CURSOR_POSITION = "Invalid cursor position";
	private static final String MESSAGE_INVALID_OPERATION = "Invalid operation while on insert row";
	private static final String MESSAGE_OPERATION_NOT_YET_SUPPORTED = "Operation not yet supported";
	private static final String MESSAGE_OPERATION_NOT_SUPPORTED = "Operation not supported by Database";
	private static final String MESSAGE_MATCH_COLUMNS = "Match columns are not the same as those set";
	private static final String MESSAGE_FEATURE_NOT_SUPPORTED = "Feature not supported";
	private static final String MESSAGE_MAX_ROWS_EXCEEDED = "Populating rows setting has exceeded max row setting";

	/**
	 * The <code>Connection</code> object that connects with this
	 * <code>CachedRowSetImpl</code> object's current underlying data source.
	 */
	private transient Connection conn;

	/**
	 * The <code>ResultSetMetaData</code> object that contains information about the
	 * columns in the <code>ResultSet</code> object that is the current source of
	 * data for this <code>CachedRowSetImpl</code> object.
	 */
	private transient ResultSetMetaData resultSetMetaData;

	/**
	 * The <code>RowSetMetaData</code> object that contains information about the
	 * columns in this <code>CachedRowSetImpl</code> object.
	 * 
	 * @serial
	 */
	private RowSetMetaData rowSetMetaData;

	// Properties of this RowSet

	/**
	 * An array containing the columns in this <code>CachedRowSetImpl</code> object
	 * that form a unique identifier for a row. This array is used by the writer.
	 * 
	 * @serial
	 */
	private int[] keyCols;

	/**
	 * The name of the table in the underlying database to which updates should be
	 * written. This name is needed because most drivers do not return this
	 * information in a <code>ResultSetMetaData</code> object.
	 * 
	 * @serial
	 */
	private String tableName;

	/**
	 * A <code>Vector</code> object containing the <code>Row</code> objects that
	 * comprise this <code>CachedRowSetImpl</code> object.
	 * 
	 * @serial
	 */
	private List<Object> rvh;

	/**
	 * The current position of the cursor in this <code>CachedRowSetImpl</code>
	 * object.
	 * 
	 * @serial
	 */
	private int cursorPos;

	/**
	 * The current position of the cursor in this <code>CachedRowSetImpl</code>
	 * object not counting rows that have been deleted, if any.
	 * <P>
	 * For example, suppose that the cursor is on the last row of a rowset that
	 * started with five rows and subsequently had the second and third rows
	 * deleted. The <code>absolutePos</code> would be <code>3</code>, whereas the
	 * <code>cursorPos</code> would be <code>5</code>.
	 * 
	 * @serial
	 */
	private int absolutePos;

	/**
	 * The number of deleted rows currently in this <code>CachedRowSetImpl</code>
	 * object.
	 * 
	 * @serial
	 */
	private int numDeleted;

	/**
	 * The total number of rows currently in this <code>CachedRowSetImpl</code>
	 * object.
	 * 
	 * @serial
	 */
	private int numRows;

	/**
	 * A special row used for constructing a new row. A new row is constructed by
	 * using <code>ResultSet.updateXXX</code> methods to insert column values into
	 * the insert row.
	 * 
	 * @serial
	 */
	private InsertRow insertRow;

	/**
	 * A <code>boolean</code> indicating whether the cursor is currently on the
	 * insert row.
	 * 
	 * @serial
	 */
	private boolean onInsertRow;

	/**
	 * The field that temporarily holds the last position of the cursor before it
	 * moved to the insert row, thus preserving the number of the current row to
	 * which the cursor may return.
	 * 
	 * @serial
	 */
	private int currentRow;

	/**
	 * A <code>boolean</code> indicating whether the last value returned was an SQL
	 * <code>NULL</code>.
	 * 
	 * @serial
	 */
	private boolean lastValueNull;

	/**
	 * A <code>SQLWarning</code> which logs on the warnings
	 */
	private SQLWarning sqlwarn;

	/**
	 * A <code>RowSetWarning</code> which logs on the warnings
	 */
	private RowSetWarning rowsetWarning;

	/**
	 * The boolean variable indicating locatorsUpdateValue
	 */
	private boolean dbmslocatorsUpdateCopy;

	/**
	 * The <code>ResultSet</code> object that is used to maintain the data when a
	 * ResultSet and start position are passed as parameters to the populate
	 * function
	 */
	private transient ResultSet resultSet;

	/**
	 * The integer value indicating the end position in the ResultSetwhere the
	 * picking up of rows for populating a CachedRowSet object was left off.
	 */
	private int endPos;

	/**
	 * The integer value indicating the end position in the ResultSetwhere the
	 * picking up of rows for populating a CachedRowSet object was left off.
	 */
	private int prevEndPos;

	/**
	 * The integer value indicating the position in the ResultSet, to populate the
	 * CachedRowSet object.
	 */
	private int startPos;

	/**
	 * The integer value indicating the position from where the page prior to this
	 * was populated.
	 */
	private int startPrev;

	/**
	 * The integer value indicating size of the page.
	 */
	private int pageSize;

	/**
	 * The integer value indicating number of rows that have been processed so far.
	 * Used for checking whether maxRows has been reached or not.
	 */
	private int maxRowsreached;
	/**
	 * The boolean value when true signifies that pages are still to follow and a
	 * false value indicates that this is the last page.
	 */
	private boolean pagenotend = true;

	/**
	 * The boolean value indicating whether this is the first page or not.
	 */
	private boolean onFirstPage;

	/**
	 * The integer value indicating how many times the populate function has been
	 * called.
	 */
	private int populatecallcount;

	/**
	 * The integer value indicating the total number of rows to be processed in the
	 * ResultSet object passed to the populate function.
	 */
	private int totalRows;

	/**
	 * The boolean value indicating how the CahedRowSet object has been populated
	 * for paging purpose. True indicates that connection parameter is passed.
	 */
	private boolean callWithCon;

	/**
	 * The Vector holding the Match Columns
	 */
	private List<Integer> iMatchColumns;

	/**
	 * The Vector that will hold the Match Column names.
	 */
	private List<String> strMatchColumns;

	/**
	 * Constructs a new default <code>CachedRowSetImpl</code> object with the
	 * capacity to hold 100 rows. This new object has no metadata and has the
	 * following default values:
	 * 
	 * <pre>
	 *     onInsertRow = false
	 *     insertRow = null
	 *     cursorPos = 0
	 *     numRows = 0
	 *     showDeleted = false
	 *     queryTimeout = 0
	 *     maxRows = 0
	 *     maxFieldSize = 0
	 *     rowSetType = ResultSet.TYPE_SCROLL_INSENSITIVE
	 *     concurrency = ResultSet.CONCUR_UPDATABLE
	 *     readOnly = false
	 *     isolation = Connection.TRANSACTION_READ_COMMITTED
	 *     escapeProcessing = true
	 *     onInsertRow = false
	 *     insertRow = null
	 *     cursorPos = 0
	 *     absolutePos = 0
	 *     numRows = 0
	 * </pre>
	 * 
	 * A <code>CachedRowSetImpl</code> object is configured to use the default
	 * <code>RIOptimisticProvider</code> implementation to provide connectivity and
	 * synchronization capabilities to the set data source.
	 * <P>
	 * 
	 * @throws SQLException if an error occurs
	 */
	public CachedRowSetImpl() throws SQLException {

		// allocate the parameters collection
		initParams();

		initContainer();

		// set up some default values
		initProperties();

		// insert row setup
		onInsertRow = false;
		insertRow = null;

		// set the warninings
		sqlwarn = new SQLWarning();
		rowsetWarning = new RowSetWarning();

	}

	/**
	 * Sets the <code>rvh</code> field to a new <code>Vector</code> object with a
	 * capacity of 100 and sets the <code>cursorPos</code> and <code>numRows</code>
	 * fields to zero.
	 */
	private void initContainer() {

		rvh = new ArrayList<>(100);
		cursorPos = 0;
		absolutePos = 0;
		numRows = 0;
		numDeleted = 0;
	}

	/**
	 * Sets the properties for this <code>CachedRowSetImpl</code> object to their
	 * default values. This method is called internally by the default constructor.
	 */

	private void initProperties() throws SQLException {

		setShowDeleted(false);
		setQueryTimeout(0);
		setMaxRows(0);
		setMaxFieldSize(0);
		setType(ResultSet.TYPE_SCROLL_INSENSITIVE);
		setConcurrency(ResultSet.CONCUR_UPDATABLE);
		setReadOnly(rvh.isEmpty() || isReadOnly());
		setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		setEscapeProcessing(true);

		// Instantiating the vector for MatchColumns

		iMatchColumns = new ArrayList<>(10);
		for (int i = 0; i < 10; i++) {
			iMatchColumns.add(i, -1);
		}

		strMatchColumns = new ArrayList<>(10);
		for (int j = 0; j < 10; j++) {
			strMatchColumns.add(j, null);
		}
	}

	// -----------------------------------------------------------------------
	// Properties
	// -----------------------------------------------------------------------

	/**
	 * Sets this <code>CachedRowSetImpl</code> object's command property to the
	 * given <code>String</code> object and clears the parameters, if any, that were
	 * set for the previous command.
	 * <P>
	 * The command property may not be needed if the rowset is produced by a data
	 * source, such as a spreadsheet, that does not support commands. Thus, this
	 * property is optional and may be <code>null</code>.
	 *
	 * @param cmd a <code>String</code> object containing an SQL query that will be
	 *            set as the command; may be <code>null</code>
	 * @throws SQLException if an error occurs
	 */
	@Override
	public void setCommand(String cmd) throws SQLException {
		super.setCommand(cmd);
		if (!buildTableName(cmd).isEmpty()) {
			this.setTableName(buildTableName(cmd));
		}
	}

	// ---------------------------------------------------------------------
	// Reading and writing data
	// ---------------------------------------------------------------------

	/**
	 * Populates this <code>CachedRowSetImpl</code> object with data from the given
	 * <code>ResultSet</code> object. This method is an alternative to the method
	 * <code>execute</code> for filling the rowset with data. The method
	 * <code>populate</code> does not require that the properties needed by the
	 * method <code>execute</code>, such as the <code>command</code> property, be
	 * set. This is true because the method <code>populate</code> is given the
	 * <code>ResultSet</code> object from which to get data and thus does not need
	 * to use the properties required for setting up a connection and executing this
	 * <code>CachedRowSetImpl</code> object's command.
	 * <P>
	 * After populating this rowset with data, the method <code>populate</code> sets
	 * the rowset's metadata and then sends a <code>RowSetChangedEvent</code> object
	 * to all registered listeners prior to returning.
	 *
	 * @param data the <code>ResultSet</code> object containing the data to be read
	 *             into this <code>CachedRowSetImpl</code> object
	 * @throws SQLException if an error occurs; or the max row setting is violated
	 *                      while populating the RowSet
	 * @see #execute
	 */
	@Override
	public void populate(ResultSet data) throws SQLException {
		int rowsFetched;
		Row row;
		int numCols;
		int i;
		Map<String, Class<?>> map = getTypeMap();
		Object obj;
		int mRows;

		if (data == null) {
			throw new SQLException("Invalid ResultSet object supplied to populate method");
		}
		this.resultSet = data;

		// get the meta data for this ResultSet
		resultSetMetaData = data.getMetaData();

		// set up the metadata
		rowSetMetaData = new RowSetMetaDataImpl();
		initMetaData(rowSetMetaData, resultSetMetaData);

		// release the meta-data so that aren't tempted to use it.
		resultSetMetaData = null;
		numCols = rowSetMetaData.getColumnCount();
		mRows = this.getMaxRows();
		rowsFetched = 0;

		while (data.next()) {

			row = new Row(numCols);

			if (rowsFetched > mRows && mRows > 0) {
				rowsetWarning.setNextWarning(new RowSetWarning(MESSAGE_MAX_ROWS_EXCEEDED));
			}
			for (i = 1; i <= numCols; i++) {
				obj = getValue(rowSetMetaData, i, data, map);
				/*
				 * the following block checks for the various types that we have to serialize in
				 * order to store - right now only structs have been tested
				 */
				if (obj instanceof Struct) {
					obj = new SerialStruct((Struct) obj, map);
				} else if (obj instanceof SQLData) {
					obj = new SerialStruct((SQLData) obj, map);
				} else if (obj instanceof Blob) {
					obj = new SerialBlob((Blob) obj);
				} else if (obj instanceof Clob) {
					obj = new SerialClob((Clob) obj);
				} else if (obj instanceof java.sql.Array) {
					if (map != null)
						obj = new SerialArray((java.sql.Array) obj, map);
					else
						obj = new SerialArray((java.sql.Array) obj);
				}

				row.initColumnObject(i, obj);
			}
			rowsFetched++;
			rvh.add(row);
		}

		numRows = rowsFetched;
		// Also rowsFetched should be equal to rvh.size()

		// notify any listeners that the rowset has changed
		notifyRowSetChanged();

	}

	private Object getValue(RowSetMetaData metaData, int columnIndex, ResultSet data, Map<String, Class<?>> map)
			throws SQLException {
		int columnType = metaData.getColumnType(columnIndex);
		switch (columnType) {
		case Types.TIMESTAMP:
			return data.getTimestamp(columnIndex);
		case Types.TIME:
			return data.getTime(columnIndex);
		case Types.DATE:
			return data.getDate(columnIndex);
		}
		/*
		 * check if the user has set a map. If no map is set then use plain getObject.
		 * This lets us work with drivers that do not support getObject with a map in
		 * fairly sensible way
		 */
		if (map == null || map.isEmpty()) {
			return data.getObject(columnIndex);
		}
		return data.getObject(columnIndex, map);
	}

	/**
	 * Initializes the given <code>RowSetMetaData</code> object with the values in
	 * the given <code>ResultSetMetaData</code> object.
	 *
	 * @param md   the <code>RowSetMetaData</code> object for this
	 *             <code>CachedRowSetImpl</code> object, which will be set with
	 *             values from rsmd
	 * @param rsmd the <code>ResultSetMetaData</code> object from which new values
	 *             for md will be read
	 * @throws SQLException if an error occurs
	 */
	private void initMetaData(RowSetMetaData md, ResultSetMetaData rsmd) throws SQLException {
		int numCols = rsmd.getColumnCount();

		md.setColumnCount(numCols);
		for (int col = 1; col <= numCols; col++) {
			md.setAutoIncrement(col, rsmd.isAutoIncrement(col));
			md.setCaseSensitive(col, rsmd.isCaseSensitive(col));
			md.setCurrency(col, rsmd.isCurrency(col));
			md.setNullable(col, rsmd.isNullable(col));
			md.setSigned(col, rsmd.isSigned(col));
			md.setSearchable(col, rsmd.isSearchable(col));
			/*
			 * The PostgreSQL drivers sometimes return negative columnDisplaySize, which
			 * causes an exception to be thrown. Check for it.
			 */
			int size = rsmd.getColumnDisplaySize(col);
			if (size < 0) {
				size = 0;
			}
			md.setColumnDisplaySize(col, size);
			md.setColumnLabel(col, rsmd.getColumnLabel(col));
			md.setColumnName(col, rsmd.getColumnName(col));
			md.setSchemaName(col, rsmd.getSchemaName(col));
			/*
			 * Drivers return some strange values for precision, for non-numeric data,
			 * including reports of non-integer values; maybe we should check type, & set to
			 * 0 for non-numeric types.
			 */
			int precision = rsmd.getPrecision(col);
			if (precision < 0) {
				precision = 0;
			}
			md.setPrecision(col, precision);

			/*
			 * It seems, from a bug report, that a driver can sometimes return a negative
			 * value for scale. javax.sql.rowset.RowSetMetaDataImpl will throw an exception
			 * if we attempt to set a negative value. As such, we'll check for this case.
			 */
			int scale = rsmd.getScale(col);
			if (scale < 0) {
				scale = 0;
			}
			md.setScale(col, scale);
			md.setTableName(col, rsmd.getTableName(col));
			md.setCatalogName(col, rsmd.getCatalogName(col));
			md.setColumnType(col, rsmd.getColumnType(col));
			md.setColumnTypeName(col, rsmd.getColumnTypeName(col));
		}

		if (conn != null) {
			// JDBC 4.0 mandates as does the Java EE spec that all DataBaseMetaData methods
			// must be implemented, therefore, the previous fix for 5055528 is being backed
			// out
			dbmslocatorsUpdateCopy = conn.getMetaData().locatorsUpdateCopy();
		}
	}

	/**
	 * Populates this <code>CachedRowSetImpl</code> object with data, using the
	 * given connection to produce the result set from which data will be read. A
	 * second form of this method, which takes no arguments, uses the values from
	 * this rowset's user, password, and either url or data source properties to
	 * create a new database connection. The form of <code>execute</code> that is
	 * given a connection ignores these properties.
	 *
	 * @param conn A standard JDBC <code>Connection</code> object that this
	 *             <code>CachedRowSet</code> object can pass to a synchronization
	 *             provider to establish a connection to the data source
	 * @throws SQLException if an invalid <code>Connection</code> is supplied or an
	 *                      error occurs in establishing the connection to the data
	 *                      source
	 * @see #populate
	 * @see java.sql.Connection
	 */
	public void execute(Connection conn) throws SQLException {
		// store the connection so the reader can find it.
		setConnection(conn);

		throw new UnsupportedOperationException("readData not supported");
	}

	/**
	 * Sets this <code>CachedRowSetImpl</code> object's connection property to the
	 * given <code>Connection</code> object. This method is called internally by the
	 * version of the method <code>execute</code> that takes a
	 * <code>Connection</code> object as an argument. The reader for this
	 * <code>CachedRowSetImpl</code> object can retrieve the connection stored in
	 * the rowset's connection property by calling its <code>getConnection</code>
	 * method.
	 *
	 * @param connection the <code>Connection</code> object that was passed in to
	 *                   the method <code>execute</code> and is to be stored in this
	 *                   <code>CachedRowSetImpl</code> object's connection property
	 */
	private void setConnection(Connection connection) {
		conn = connection;
	}

	/**
	 * Propagates all row update, insert, and delete changes to the underlying data
	 * source backing this <code>CachedRowSetImpl</code> object.
	 * <P>
	 * <b>Note</b>In the reference implementation an optimistic concurrency
	 * implementation is provided as a sample implementation of a the
	 * <code>SyncProvider</code> abstract class.
	 * <P>
	 * This method fails if any of the updates cannot be propagated back to the data
	 * source. When it fails, the caller can assume that none of the updates are
	 * reflected in the data source. When an exception is thrown, the current row is
	 * set to the first "updated" row that resulted in an exception unless the row
	 * that caused the exception is a "deleted" row. In that case, when deleted rows
	 * are not shown, which is usually true, the current row is not affected.
	 * <P>
	 * If no <code>SyncProvider</code> is configured, the reference implementation
	 * leverages the <code>RIOptimisticProvider</code> available which provides the
	 * default and reference synchronization capabilities for disconnected
	 * <code>RowSets</code>.
	 *
	 * @throws SQLException          if the cursor is on the insert row or the
	 *                               underlying reference synchronization provider
	 *                               fails to commit the updates to the datasource
	 * @throws SyncProviderException if an internal error occurs within the
	 *                               <code>SyncProvider</code> instance during
	 *                               either during the process or at any time when
	 *                               the <code>SyncProvider</code> instance touches
	 *                               the data source.
	 * @see #acceptChanges(java.sql.Connection)
	 * @see javax.sql.RowSetWriter
	 * @see javax.sql.rowset.spi.SyncProvider
	 */
	public void acceptChanges() throws SyncProviderException {
		if (onInsertRow) {
			throw new SyncProviderException(MESSAGE_INVALID_OPERATION);
		}

		try {

			throw new SyncProviderException("acceptChanges Failed");

		} catch (SyncProviderException spe) {
			throw spe;
		} catch (SecurityException e) {
			throw new SyncProviderException(e.getMessage());
		}
	}

	/**
	 * Propagates all row update, insert, and delete changes to the data source
	 * backing this <code>CachedRowSetImpl</code> object using the given
	 * <code>Connection</code> object.
	 * <P>
	 * The reference implementation <code>RIOptimisticProvider</code> modifies its
	 * synchronization to a write back function given the updated connection The
	 * reference implementation modifies its synchronization behaviour via the
	 * <code>SyncProvider</code> to ensure the synchronization occurs according to
	 * the updated JDBC <code>Connection</code> properties.
	 *
	 * @param con a standard JDBC <code>Connection</code> object
	 * @throws SQLException if the cursor is on the insert row or the underlying
	 *                      synchronization provider fails to commit the updates
	 *                      back to the data source
	 * @see #acceptChanges
	 * @see javax.sql.RowSetWriter
	 * @see javax.sql.rowset.spi.SyncFactory
	 * @see javax.sql.rowset.spi.SyncProvider
	 */
	public void acceptChanges(Connection con) throws SyncProviderException {
		setConnection(con);
		acceptChanges();
	}

	/**
	 * Restores this <code>CachedRowSetImpl</code> object to its original state,
	 * that is, its state before the last set of changes.
	 * <P>
	 * Before returning, this method moves the cursor before the first row and sends
	 * a <code>rowSetChanged</code> event to all registered listeners.
	 * 
	 * @throws SQLException if an error is occurs rolling back the RowSet state to
	 *                      the definied original value.
	 * @see javax.sql.RowSetListener#rowSetChanged
	 */
	public void restoreOriginal() throws SQLException {
		Row row;
		for (Iterator<?> i = rvh.iterator(); i.hasNext();) {
			row = (Row) i.next();
			if (row.getInserted()) {
				i.remove();
				--numRows;
			} else {
				if (row.getDeleted()) {
					row.clearDeleted();
				}
				if (row.getUpdated()) {
					row.clearUpdated();
				}
			}
		}
		// move to before the first
		cursorPos = 0;

		// notify any listeners
		notifyRowSetChanged();
	}

	/**
	 * Releases the current contents of this <code>CachedRowSetImpl</code> object
	 * and sends a <code>rowSetChanged</code> event object to all registered
	 * listeners.
	 *
	 * @throws SQLException if an error occurs flushing the contents of RowSet.
	 * @see javax.sql.RowSetListener#rowSetChanged
	 */
	public void release() throws SQLException {
		initContainer();
		notifyRowSetChanged();
	}

	/**
	 * Cancels deletion of the current row and notifies listeners that a row has
	 * changed.
	 * <P>
	 * Note: This method can be ignored if deleted rows are not being shown, which
	 * is the normal case.
	 *
	 * @throws SQLException if the cursor is not on a valid row
	 */
	public void undoDelete() throws SQLException {
		if (!getShowDeleted()) {
			return;
		}
		// make sure we are on a row
		checkCursor();

		// don't want this to happen...
		if (onInsertRow) {
			throw new SQLException(MESSAGE_INVALID_CURSOR_POSITION);
		}

		Row row = (Row) getCurrentRow();
		if (row.getDeleted()) {
			row.clearDeleted();
			--numDeleted;
			notifyRowChanged();
		}
	}

	/**
	 * Immediately removes the current row from this <code>CachedRowSetImpl</code>
	 * object if the row has been inserted, and also notifies listeners the a row
	 * has changed. An exception is thrown if the row is not a row that has been
	 * inserted or the cursor is before the first row, after the last row, or on the
	 * insert row.
	 * <P>
	 * This operation cannot be undone.
	 *
	 * @throws SQLException if an error occurs, the cursor is not on a valid row, or
	 *                      the row has not been inserted
	 */
	public void undoInsert() throws SQLException {
		// make sure we are on a row
		checkCursor();

		// don't want this to happen...
		if (onInsertRow) {
			throw new SQLException(MESSAGE_INVALID_CURSOR_POSITION);
		}

		Row row = (Row) getCurrentRow();
		if (row.getInserted()) {
			rvh.remove(cursorPos - 1);
			--numRows;
			notifyRowChanged();
		} else {
			throw new SQLException("Illegal operation on non-inserted row");
		}
	}

	/**
	 * Immediately reverses the last update operation if the row has been modified.
	 * This method can be called to reverse updates on a all columns until all
	 * updates in a row have been rolled back to their originating state since the
	 * last synchronization (<code>acceptChanges</code>) or population. This method
	 * may also be called while performing updates to the insert row.
	 * <P>
	 * {@code undoUpdate} may be called at any time during the life-time of a
	 * rowset, however after a synchronization has occurs this method has no affect
	 * until further modification to the RowSet data occurs.
	 *
	 * @throws SQLException if cursor is before the first row, after the last row in
	 *                      rowset.
	 * @see #undoDelete
	 * @see #undoInsert
	 * @see java.sql.ResultSet#cancelRowUpdates
	 */
	public void undoUpdate() throws SQLException {
		// if on insert row, cancel the insert row
		// make the insert row flag,
		// cursorPos back to the current row
		moveToCurrentRow();

		// else if not on insert row
		// call undoUpdate or undoInsert
		undoDelete();

		undoInsert();

	}

	// --------------------------------------------------------------------
	// Views
	// --------------------------------------------------------------------

	/**
	 * Returns a new <code>RowSet</code> object backed by the same data as that of
	 * this <code>CachedRowSetImpl</code> object and sharing a set of cursors with
	 * it. This allows cursors to interate over a shared set of rows, providing
	 * multiple views of the underlying data.
	 *
	 * @return a <code>RowSet</code> object that is a copy of this
	 *         <code>CachedRowSetImpl</code> object and shares a set of cursors with
	 *         it
	 * @throws SQLException if an error occurs or cloning is not supported
	 * @see javax.sql.RowSetEvent
	 * @see javax.sql.RowSetListener
	 */
	public RowSet createShared() throws SQLException {
		RowSet clone;
		try {
			clone = (RowSet) clone();
		} catch (CloneNotSupportedException ex) {
			throw new SQLException(ex.getMessage());
		}
		return clone;
	}

	/**
	 * Creates a <code>RowSet</code> object that is a deep copy of this
	 * <code>CachedRowSetImpl</code> object's data, including constraints. Updates
	 * made on a copy are not visible to the original rowset; a copy of a rowset is
	 * completely independent from the original.
	 * <P>
	 * Making a copy saves the cost of creating an identical rowset from first
	 * principles, which can be quite expensive. For example, it can eliminate the
	 * need to query a remote database server.
	 * 
	 * @return a new <code>CachedRowSet</code> object that is a deep copy of this
	 *         <code>CachedRowSet</code> object and is completely independent from
	 *         this <code>CachedRowSetImpl</code> object.
	 * @throws SQLException if an error occurs in generating the copy of this of the
	 *                      <code>CachedRowSetImpl</code>
	 * @see #createShared
	 * @see javax.sql.RowSetEvent
	 * @see javax.sql.RowSetListener
	 */
	public CachedRowSet createCopy() throws SQLException {
		ObjectOutputStream out;
		ByteArrayOutputStream bOut = new ByteArrayOutputStream();
		try {
			out = new ObjectOutputStream(bOut);
			out.writeObject(this);
		} catch (IOException ex) {
			throw new SQLException(cloneFailMessage(ex));
		}

		ObjectInputStream in;

		try {
			ByteArrayInputStream bIn = new ByteArrayInputStream(bOut.toByteArray());
			in = new ObjectInputStream(bIn);
		} catch (IOException ex) {
			throw new SQLException(cloneFailMessage(ex));
		}

		try {
			return (CachedRowSet) in.readObject();

		} catch (Exception ex) {
			throw new SQLException(cloneFailMessage(ex));
		}
	}

	private String cloneFailMessage(Exception ex) {
		return MessageFormat.format(MESSAGE_CLONE_FAIL, ex.getMessage());
	}

	/**
	 * Creates a <code>RowSet</code> object that is a copy of this
	 * <code>CachedRowSetImpl</code> object's table structure and the constraints
	 * only. There will be no data in the object being returned. Updates made on a
	 * copy are not visible to the original rowset.
	 * <P>
	 * This helps in getting the underlying XML schema which can be used as the
	 * basis for populating a <code>WebRowSet</code>.
	 *
	 * @return a new <code>CachedRowSet</code> object that is a copy of this
	 *         <code>CachedRowSetImpl</code> object's schema and retains all the
	 *         constraints on the original rowset but contains no data
	 * @throws SQLException if an error occurs in generating the copy of the
	 *                      <code>CachedRowSet</code> object
	 * @see #createShared
	 * @see #createCopy
	 * @see #createCopyNoConstraints
	 * @see javax.sql.RowSetEvent
	 * @see javax.sql.RowSetListener
	 */
	public CachedRowSet createCopySchema() throws SQLException {
		// Copy everything except data i.e all constraints

		// Store the number of rows of "this"
		// and make numRows equals zero.
		// and make data also zero.
		int nRows = numRows;
		numRows = 0;

		CachedRowSet crs = this.createCopy();

		// reset this object back to number of rows.
		numRows = nRows;

		return crs;
	}

	/**
	 * Creates a <code>CachedRowSet</code> object that is a copy of this
	 * <code>CachedRowSetImpl</code> object's data only. All constraints set in this
	 * object will not be there in the returning object. Updates made on a copy are
	 * not visible to the original rowset.
	 *
	 * @return a new <code>CachedRowSet</code> object that is a deep copy of this
	 *         <code>CachedRowSetImpl</code> object and is completely independent
	 *         from this <code>CachedRowSetImpl</code> object
	 * @throws SQLException if an error occurs in generating the copy of the of the
	 *                      <code>CachedRowSet</code>
	 * @see #createShared
	 * @see #createCopy
	 * @see #createCopySchema
	 * @see javax.sql.RowSetEvent
	 * @see javax.sql.RowSetListener
	 */
	public CachedRowSet createCopyNoConstraints() throws SQLException {
		// Copy the whole data ONLY without any constraints.
		CachedRowSetImpl crs;
		crs = (CachedRowSetImpl) this.createCopy();

		crs.initProperties();
		try {
			crs.unsetMatchColumn(crs.getMatchColumnIndexes());
		} catch (SQLException sqle) {
			// do nothing, if the setMatchColumn is not set.
		}

		try {
			crs.unsetMatchColumn(crs.getMatchColumnNames());
		} catch (SQLException sqle) {
			// do nothing, if the setMatchColumn is not set.
		}

		return crs;
	}

	/**
	 * Converts this <code>CachedRowSetImpl</code> object to a collection of tables.
	 * The sample implementation utilitizes the <code>TreeMap</code> collection
	 * type. This class guarantees that the map will be in ascending key order,
	 * sorted according to the natural order for the key's class.
	 *
	 * @return a <code>Collection</code> object consisting of tables, each of which
	 *         is a copy of a row in this <code>CachedRowSetImpl</code> object
	 * @throws SQLException if an error occurs in generating the collection
	 * @see #toCollection(int)
	 * @see #toCollection(String)
	 * @see java.util.TreeMap
	 */
	public Collection<?> toCollection() throws SQLException {

		TreeMap<Integer, Object> tMap = new TreeMap<>();

		for (int i = 0; i < numRows; i++) {
			tMap.put(i, rvh.get(i));
		}

		return (tMap.values());
	}

	/**
	 * Returns the specified column of this <code>CachedRowSetImpl</code> object as
	 * a <code>Collection</code> object. This method makes a copy of the column's
	 * data and utilitizes the <code>Vector</code> to establish the collection. The
	 * <code>Vector</code> class implements a growable array objects allowing the
	 * individual components to be accessed using an an integer index similar to
	 * that of an array.
	 *
	 * @return a <code>Collection</code> object that contains the value(s) stored in
	 *         the specified column of this <code>CachedRowSetImpl</code> object
	 * @throws SQLException if an error occurs generated the collection; or an
	 *                      invalid column is provided.
	 * @see #toCollection()
	 * @see #toCollection(String)
	 * @see java.util.Vector
	 */
	public Collection<?> toCollection(int column) throws SQLException {

		int nRows = numRows;
		List<Object> vec = new ArrayList<>(nRows);

		// create a copy
		CachedRowSetImpl crsTemp;
		crsTemp = (CachedRowSetImpl) this.createCopy();

		while (nRows != 0) {
			crsTemp.next();
			vec.add(crsTemp.getObject(column));
			nRows--;
		}

		return vec;
	}

	/**
	 * Returns the specified column of this <code>CachedRowSetImpl</code> object as
	 * a <code>Collection</code> object. This method makes a copy of the column's
	 * data and utilitizes the <code>Vector</code> to establish the collection. The
	 * <code>Vector</code> class implements a growable array objects allowing the
	 * individual components to be accessed using an an integer index similar to
	 * that of an array.
	 *
	 * @return a <code>Collection</code> object that contains the value(s) stored in
	 *         the specified column of this <code>CachedRowSetImpl</code> object
	 * @throws SQLException if an error occurs generated the collection; or an
	 *                      invalid column is provided.
	 * @see #toCollection()
	 * @see #toCollection(int)
	 * @see java.util.Vector
	 */
	public Collection<?> toCollection(String column) throws SQLException {
		return toCollection(getColIdxByName(column));
	}

	// --------------------------------------------------------------------
	// Advanced features
	// --------------------------------------------------------------------

	/**
	 * Returns the <code>SyncProvider</code> implementation being used with this
	 * <code>CachedRowSetImpl</code> implementation rowset.
	 *
	 * @return the SyncProvider used by the rowset. If not provider was set when the
	 *         rowset was instantiated, the reference implementation (default)
	 *         provider is returned.
	 * @throws SQLException if error occurs while return the
	 *                      <code>SyncProvider</code> instance.
	 */
	public SyncProvider getSyncProvider() throws SQLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Sets the active <code>SyncProvider</code> and attempts to load load the new
	 * provider using the <code>SyncFactory</code> SPI.
	 *
	 * @throws SQLException if an error occurs while resetting the
	 *                      <code>SyncProvider</code>.
	 */
	public void setSyncProvider(String providerStr) throws SQLException {
		throw new UnsupportedOperationException();
	}

	// -----------------
	// methods inherited from RowSet
	// -----------------

	// ---------------------------------------------------------------------
	// Reading and writing data
	// ---------------------------------------------------------------------

	/**
	 * Populates this <code>CachedRowSetImpl</code> object with data. This form of
	 * the method uses the rowset's user, password, and url or data source name
	 * properties to create a database connection. If properties that are needed
	 * have not been set, this method will throw an exception.
	 * <P>
	 * Another form of this method uses an existing JDBC <code>Connection</code>
	 * object instead of creating a new one; therefore, it ignores the properties
	 * used for establishing a new connection.
	 * <P>
	 * The query specified by the command property is executed to create a
	 * <code>ResultSet</code> object from which to retrieve data. The current
	 * contents of the rowset are discarded, and the rowset's metadata is also
	 * (re)set. If there are outstanding updates, they are also ignored.
	 * <P>
	 * The method <code>execute</code> closes any database connections that it
	 * creates.
	 *
	 * @throws SQLException if an error occurs or the necessary properties have not
	 *                      been set
	 */
	public void execute() throws SQLException {
		execute(null);
	}

	// -----------------------------------
	// Methods inherited from ResultSet
	// -----------------------------------

	/**
	 * Moves the cursor down one row from its current position and returns
	 * <code>true</code> if the new cursor position is a valid row. The cursor for a
	 * new <code>ResultSet</code> object is initially positioned before the first
	 * row. The first call to the method <code>next</code> moves the cursor to the
	 * first row, making it the current row; the second call makes the second row
	 * the current row, and so on.
	 *
	 * <P>
	 * If an input stream from the previous row is open, it is implicitly closed.
	 * The <code>ResultSet</code> object's warning chain is cleared when a new row
	 * is read.
	 *
	 * @return <code>true</code> if the new current row is valid; <code>false</code>
	 *         if there are no more rows
	 * @throws SQLException if an error occurs or the cursor is not positioned in
	 *                      the rowset, before the first row, or after the last row
	 */
	public boolean next() throws SQLException {
		/*
		 * make sure things look sane. The cursor must be positioned in the rowset or
		 * before first (0) or after last (numRows + 1)
		 */
		if (cursorPos < 0 || cursorPos >= numRows + 1) {
			throw new SQLException(MESSAGE_INVALID_CURSOR_POSITION);
		}
		// now move and notify
		boolean ret = this.internalNext();
		notifyCursorMoved();

		return ret;
	}

	/**
	 * Moves this <code>CachedRowSetImpl</code> object's cursor to the next row and
	 * returns <code>true</code> if the cursor is still in the rowset; returns
	 * <code>false</code> if the cursor has moved to the position after the last
	 * row.
	 * <P>
	 * This method handles the cases where the cursor moves to a row that has been
	 * deleted. If this rowset shows deleted rows and the cursor moves to a row that
	 * has been deleted, this method moves the cursor to the next row until the
	 * cursor is on a row that has not been deleted.
	 * <P>
	 * The method <code>internalNext</code> is called by methods such as
	 * <code>next</code>, <code>absolute</code>, and <code>relative</code>, and, as
	 * its name implies, is only called internally.
	 * <p>
	 * This is a implementation only method and is not required as a standard
	 * implementation of the <code>CachedRowSet</code> interface.
	 *
	 * @return <code>true</code> if the cursor is on a valid row in this rowset;
	 *         <code>false</code> if it is after the last row
	 * @throws SQLException if an error occurs
	 */
	protected boolean internalNext() throws SQLException {
		boolean ret = false;

		do {
			if (cursorPos < numRows) {
				++cursorPos;
				ret = true;
			} else if (cursorPos == numRows) {
				// increment to after last
				++cursorPos;
				ret = false;
				break;
			}
		} while (!getShowDeleted() && rowDeleted());

		/*
		 * each call to internalNext may increment cursorPos multiple times however, the
		 * absolutePos only increments once per call.
		 */
		if (ret)
			absolutePos++;
		else
			absolutePos = 0;

		return ret;
	}

	/**
	 * Closes this <code>CachedRowSetImpl</code> objecy and releases any resources
	 * it was using.
	 *
	 * @throws SQLException if an error occurs when releasing any resources in use
	 *                      by this <code>CachedRowSetImpl</code> object
	 */
	public void close() throws SQLException {

		// close all data structures holding
		// the disconnected rowset

		cursorPos = 0;
		absolutePos = 0;
		numRows = 0;
		numDeleted = 0;

		// set all insert(s), update(s) & delete(s),
		// if at all, to their initial values.
		initProperties();

		// clear the vector of it's present contents
		rvh.clear();

	}

	/**
	 * Reports whether the last column read was SQL <code>NULL</code>. Note that you
	 * must first call the method <code>getXXX</code> on a column to try to read its
	 * value and then call the method <code>wasNull</code> to determine whether the
	 * value was SQL <code>NULL</code>.
	 *
	 * @return <code>true</code> if the value in the last column read was SQL
	 *         <code>NULL</code>; <code>false</code> otherwise
	 * @throws SQLException if an error occurs
	 */
	public boolean wasNull() throws SQLException {
		return lastValueNull;
	}

	/**
	 * Sets the field <code>lastValueNull</code> to the given <code>boolean</code>
	 * value.
	 *
	 * @param value <code>true</code> to indicate that the value of the last column
	 *              read was SQL <code>NULL</code>; <code>false</code> to indicate
	 *              that it was not
	 */
	private void setLastValueNull(boolean value) {
		lastValueNull = value;
	}

	// Methods for accessing results by column index

	/**
	 * Checks to see whether the given index is a valid column number in this
	 * <code>CachedRowSetImpl</code> object and throws an <code>SQLException</code>
	 * if it is not. The index is out of bounds if it is less than <code>1</code> or
	 * greater than the number of columns in this rowset.
	 * <P>
	 * This method is called internally by the <code>getXXX</code> and
	 * <code>updateXXX</code> methods.
	 *
	 * @param idx the number of a column in this <code>CachedRowSetImpl</code>
	 *            object; must be between <code>1</code> and the number of rows in
	 *            this rowset
	 * @throws SQLException if the given index is out of bounds
	 */
	private void checkIndex(int idx) throws SQLException {
		if (idx < 1 || rowSetMetaData == null || idx > rowSetMetaData.getColumnCount()) {
			throw new SQLException("Invalid column index");
		}
	}

	/**
	 * Checks to see whether the cursor for this <code>CachedRowSetImpl</code>
	 * object is on a row in the rowset and throws an <code>SQLException</code> if
	 * it is not.
	 * <P>
	 * This method is called internally by <code>getXXX</code> methods, by
	 * <code>updateXXX</code> methods, and by methods that update, insert, or delete
	 * a row or that cancel a row update, insert, or delete.
	 *
	 * @throws SQLException if the cursor for this <code>CachedRowSetImpl</code>
	 *                      object is not on a valid row
	 */
	private void checkCursor() throws SQLException {
		if (isAfterLast() || isBeforeFirst()) {
			throw new SQLException(MESSAGE_INVALID_CURSOR_POSITION);
		}
	}

	/**
	 * Returns the column number of the column with the given name in this
	 * <code>CachedRowSetImpl</code> object. This method throws an
	 * <code>SQLException</code> if the given name is not the name of one of the
	 * columns in this rowset.
	 *
	 * @param name a <code>String</code> object that is the name of a column in this
	 *             <code>CachedRowSetImpl</code> object
	 * @throws SQLException if the given name does not match the name of one of the
	 *                      columns in this rowset
	 */
	private int getColIdxByName(String name) throws SQLException {
		rowSetMetaData = (RowSetMetaDataImpl) this.getMetaData();
		int cols = rowSetMetaData.getColumnCount();
		if (rowSetMetaData != null) {
			for (int i = 1; i <= cols; ++i) {
				String colName = rowSetMetaData.getColumnName(i);
				if (colName != null && name.equalsIgnoreCase(colName)) {
					return (i);
				}
			}
		}
		throw new SQLException("Invalid column name");

	}

	/**
	 * Returns the insert row or the current row of this
	 * <code>CachedRowSetImpl</code>object.
	 *
	 * @return the <code>Row</code> object on which this
	 *         <code>CachedRowSetImpl</code> objects's cursor is positioned
	 */
	protected BaseRow getCurrentRow() {
		if (onInsertRow) {
			return insertRow;
		}
		return (BaseRow) (rvh.get(cursorPos - 1));
	}

	/**
	 * Removes the row on which the cursor is positioned.
	 * <p>
	 * This is a implementation only method and is not required as a standard
	 * implementation of the <code>CachedRowSet</code> interface.
	 *
	 * @throws SQLException if the cursor is positioned on the insert row
	 */
	protected void removeCurrentRow() {
		((Row) getCurrentRow()).setDeleted();
		rvh.remove(cursorPos - 1);
		--numRows;
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>String</code> object.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>TINYINT, SMALLINT, INTEGER, BIGINT, REAL,
	 * FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, <b>CHAR</b>, <b>VARCHAR</b></code> or
	 *                      <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type.
	 */
	public String getString(int columnIndex) throws SQLException {
		return getValue(columnIndex, null, Object::toString);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>boolean</code> value.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return the column value as a <code>boolean</code> in the Java progamming
	 *         language; if the value is SQL <code>NULL</code>, the result is
	 *         <code>false</code>
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>BOOLEAN</code> value
	 * @see #getBoolean(String)
	 */
	public boolean getBoolean(int columnIndex) throws SQLException {
		return getValue(columnIndex, false, value -> {

			// check for Boolean...
			if (value instanceof Boolean) {
				return ((Boolean) value).booleanValue();
			}

			// convert to a Double and compare to zero
			try {
				return Double.compare(Double.parseDouble(value.toString()), 0) != 0;
			} catch (NumberFormatException ex) {
				throw new SQLException(failMessage("getBoolean", value, columnIndex));
			}
		});
	}

	private String failMessage(String methodName, Object value, int columnIndex) {
		return MessageFormat.format("{0} failed on value ( {1} ) in column {2}", methodName, value.toString().trim(),
				columnIndex);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>byte</code> value.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return the column value as a <code>byte</code> in the Java programming
	 *         language; if the value is SQL <code>NULL</code>, the result is
	 *         <code>0</code>
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code><b>TINYINT</b>, SMALLINT, INTEGER, BIGINT, REAL,
	 *            FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, CHAR, VARCHAR</code> or
	 *                      <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type.
	 * @see #getByte(String)
	 */
	public byte getByte(int columnIndex) throws SQLException {
		return getNumber("getByte", columnIndex, (byte) 0, Number::byteValue, Byte::parseByte);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>short</code> value.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>0</code>
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>TINYINT, <b>SMALLINT</b>, INTEGER, BIGINT, REAL
	 * FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, CHAR, VARCHAR</code> or
	 *                      <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type.
	 * @see #getShort(String)
	 */
	public short getShort(int columnIndex) throws SQLException {
		return getNumber("getShort", columnIndex, (short) 0, Number::shortValue, Short::parseShort);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as an <code>int</code> value.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>0</code>
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>TINYINT, SMALLINT, <b>INTEGER</b>, BIGINT, REAL
	 * FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, CHAR, VARCHAR</code> or
	 *                      <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type.
	 */
	public int getInt(int columnIndex) throws SQLException {
		return getNumber("getInt", columnIndex, 0, Number::intValue, Integer::parseInt);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>long</code> value.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>0</code>
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>TINYINT, SMALLINT, INTEGER, <b>BIGINT</b>, REAL
	 * FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, CHAR, VARCHAR</code> or
	 *                      <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type.
	 * @see #getLong(String)
	 */
	public long getLong(int columnIndex) throws SQLException {
		return getNumber("getLong", columnIndex, 0l, Number::longValue, Long::parseLong);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>float</code> value.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>0</code>
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>TINYINT, SMALLINT, INTEGER, BIGINT, <b>REAL</b>,
	 * FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, CHAR, VARCHAR</code> or
	 *                      <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type.
	 * @see #getFloat(String)
	 */
	public float getFloat(int columnIndex) throws SQLException {
		return getNumber("getFloat", columnIndex, 0f, Number::floatValue, Float::parseFloat);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>double</code> value.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>0</code>
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>TINYINT, SMALLINT, INTEGER, BIGINT, REAL,
	 * <b>FLOAT</b>, <b>DOUBLE</b>, DECIMAL, NUMERIC, BIT, CHAR, VARCHAR</code> or
	 *                      <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type.
	 * @see #getDouble(String)
	 *
	 */
	public double getDouble(int columnIndex) throws SQLException {
		return getNumber("getDouble", columnIndex, 0d, Number::doubleValue, Double::parseDouble);
	}

	private <T extends Number> T getNumber(String methodName, int columnIndex, T defaultValue,
			Function<Number, T> numberConverter, Function<String, T> converter) throws SQLException {
		return getValue(columnIndex, defaultValue, value -> {
			if (value instanceof Number) {
				return numberConverter.apply((Number) value);
			}
			try {
				return converter.apply(value.toString().trim());
			} catch (NumberFormatException ex) {
				throw new SQLException(failMessage(methodName, value, columnIndex));
			}
		});
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>java.math.BigDecimal</code>
	 * object.
	 * <P>
	 * This method is deprecated; use the version of <code>getBigDecimal</code> that
	 * does not take a scale parameter and returns a value with full precision.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @param scale       the number of digits to the right of the decimal point in
	 *                    the value returned
	 * @return the column value with the specified number of digits to the right of
	 *         the decimal point; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if the given column index is out of bounds, the cursor
	 *                      is not on a valid row, or this method fails
	 * @deprecated
	 */
	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		return getValue(columnIndex, new BigDecimal(0), value -> getBigDecimal(columnIndex).setScale(scale));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>byte</code> array value.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return the column value as a <code>byte</code> array in the Java programming
	 *         language; if the value is SQL <code>NULL</code>, the result is
	 *         <code>null</code>
	 *
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code><b>BINARY</b>, <b>VARBINARY</b> or
	 * LONGVARBINARY</code> value. The bold SQL type designates the recommended
	 *                      return type.
	 * @see #getBytes(String)
	 */
	public byte[] getBytes(int columnIndex) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (!isBinary(rowSetMetaData.getColumnType(columnIndex))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		return (byte[]) (getCurrentRow().getColumnObject(columnIndex));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>java.sql.Date</code> object.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return the column value as a <code>java.sql.Data</code> object; if the value
	 *         is SQL <code>NULL</code>, the result is <code>null</code>
	 * @throws SQLException if the given column index is out of bounds, the cursor
	 *                      is not on a valid row, or this method fails
	 */
	public java.sql.Date getDate(int columnIndex) throws SQLException {
		return getValue(columnIndex, null, value -> {
			/*
			 * The object coming back from the db could be a date, a timestamp, or a char
			 * field variety. If it's a date type return it, a timestamp we turn into a long
			 * and then into a date, char strings we try to parse. Yuck.
			 */
			switch (rowSetMetaData.getColumnType(columnIndex)) {
			case java.sql.Types.DATE: {
				long sec = ((java.sql.Date) value).getTime();
				return new java.sql.Date(sec);
			}
			case java.sql.Types.TIMESTAMP: {
				long sec = ((java.sql.Timestamp) value).getTime();
				return new java.sql.Date(sec);
			}
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
				try {
					DateFormat df = DateFormat.getDateInstance();
					return ((java.sql.Date) (df.parse(value.toString())));
				} catch (ParseException ex) {
					throw new SQLException(getDateFailed(value, columnIndex));
				}
			default: {
				throw new SQLException(getDateFailed(value, columnIndex));
			}
			}
		});
	}

	private String getDateFailed(Object value, int columnIndex) {
		return failMessage("getDate", value, columnIndex);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>java.sql.Time</code> object.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if the given column index is out of bounds, the cursor
	 *                      is not on a valid row, or this method fails
	 */
	public java.sql.Time getTime(int columnIndex) throws SQLException {
		return getValue(columnIndex, null, value -> {

			/*
			 * The object coming back from the db could be a date, a timestamp, or a char
			 * field variety. If it's a date type return it, a timestamp we turn into a long
			 * and then into a date, char strings we try to parse. Yuck.
			 */
			switch (rowSetMetaData.getColumnType(columnIndex)) {
			case java.sql.Types.TIME: {
				return (java.sql.Time) value;
			}
			case java.sql.Types.TIMESTAMP: {
				long sec = ((java.sql.Timestamp) value).getTime();
				return new java.sql.Time(sec);
			}
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR: {
				try {
					DateFormat tf = DateFormat.getTimeInstance();
					return ((java.sql.Time) (tf.parse(value.toString())));
				} catch (ParseException ex) {
					throw new SQLException(failMessage("getTime", value, columnIndex));
				}
			}
			default: {
				throw new SQLException(failMessage("getTime", value, columnIndex));
			}
			}
		});
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>java.sql.Timestamp</code>
	 * object.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if the given column index is out of bounds, the cursor
	 *                      is not on a valid row, or this method fails
	 */
	public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException {
		return getValue(columnIndex, null, value -> {
			/*
			 * The object coming back from the db could be a date, a timestamp, or a char
			 * field variety. If it's a date type return it; a timestamp we turn into a long
			 * and then into a date; char strings we try to parse. Yuck.
			 */
			switch (rowSetMetaData.getColumnType(columnIndex)) {
			case java.sql.Types.TIMESTAMP: {
				return (java.sql.Timestamp) value;
			}
			case java.sql.Types.TIME: {
				long sec = ((java.sql.Time) value).getTime();
				return new java.sql.Timestamp(sec);
			}
			case java.sql.Types.DATE: {
				long sec = ((java.sql.Date) value).getTime();
				return new java.sql.Timestamp(sec);
			}
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR: {
				try {
					DateFormat tf = DateFormat.getTimeInstance();
					return ((java.sql.Timestamp) (tf.parse(value.toString())));
				} catch (ParseException ex) {
					throw new SQLException(failMessage("getTimestamp", value, columnIndex));
				}
			}
			default: {
				throw new SQLException(failMessage("getTimestamp", value, columnIndex));
			}
			}
		});
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>InputStream</code> object.
	 *
	 * A column value can be retrieved as a stream of ASCII characters and then read
	 * in chunks from the stream. This method is particularly suitable for
	 * retrieving large <code>LONGVARCHAR</code> values. The JDBC driver will do any
	 * necessary conversion from the database format into ASCII.
	 *
	 * <P>
	 * <B>Note:</B> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a get method
	 * implicitly closes the stream. . Also, a stream may return <code>0</code> for
	 * <code>CachedRowSetImpl.available()</code> whether there is data available or
	 * not.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of one-byte ASCII characters. If the value is SQL
	 *         <code>NULL</code>, the result is <code>null</code>.
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>CHAR, VARCHAR</code>,
	 *                      <code><b>LONGVARCHAR</b></code>
	 *                      <code>BINARY, VARBINARY</code> or
	 *                      <code>LONGVARBINARY</code> value. The bold SQL type
	 *                      designates the recommended return types that this method
	 *                      is used to retrieve.
	 * @see #getAsciiStream(String)
	 */
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return getValue(columnIndex, null, value -> {
			if (isString(rowSetMetaData.getColumnType(columnIndex))) {
				asciiStream = new ByteArrayInputStream(((String) value).getBytes(US_ASCII));
			} else {
				throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
			}

			return asciiStream;
		});
	}

	/**
	 * A column value can be retrieved as a stream of Unicode characters and then
	 * read in chunks from the stream. This method is particularly suitable for
	 * retrieving large LONGVARCHAR values. The JDBC driver will do any necessary
	 * conversion from the database format into Unicode.
	 *
	 * <P>
	 * <B>Note:</B> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a get method
	 * implicitly closes the stream. . Also, a stream may return 0 for available()
	 * whether there is data available or not.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of two byte Unicode characters. If the value is SQL NULL then
	 *         the result is null.
	 * @throws SQLException if an error occurs
	 * @deprecated
	 */
	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		// always free an old stream
		unicodeStream = null;

		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (!isBinary(rowSetMetaData.getColumnType(columnIndex))
				&& !isString(rowSetMetaData.getColumnType(columnIndex))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		Object value = getCurrentRow().getColumnObject(columnIndex);
		if (value == null) {
			lastValueNull = true;
			return null;
		}

		unicodeStream = new ByteArrayInputStream(value.toString().getBytes());

		return unicodeStream;
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>InputStream</code> object.
	 * <P>
	 * A column value can be retrieved as a stream of uninterpreted bytes and then
	 * read in chunks from the stream. This method is particularly suitable for
	 * retrieving large <code>LONGVARBINARY</code> values.
	 *
	 * <P>
	 * <B>Note:</B> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a get method
	 * implicitly closes the stream. Also, a stream may return <code>0</code> for
	 * <code>CachedRowSetImpl.available()</code> whether there is data available or
	 * not.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of uninterpreted bytes. If the value is SQL <code>NULL</code>
	 *         then the result is <code>null</code>.
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>BINARY, VARBINARY</code> or
	 *                      <code><b>LONGVARBINARY</b></code> The bold type
	 *                      indicates the SQL type that this method is recommened to
	 *                      retrieve.
	 * @see #getBinaryStream(String)
	 */
	public InputStream getBinaryStream(int columnIndex) throws SQLException {

		// always free an old stream
		binaryStream = null;

		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (!isBinary(rowSetMetaData.getColumnType(columnIndex))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		Object value = getCurrentRow().getColumnObject(columnIndex);
		if (value == null) {
			lastValueNull = true;
			return null;
		}

		binaryStream = new ByteArrayInputStream((byte[]) value);

		return binaryStream;

	}

	// Methods for accessing results by column name

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>String</code> object.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL
	 *                      {@code TINYINT, SMALLINT, INTEGER
	 * BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, }
	 *                      <b>{@code CHAR, VARCHAR}</b> or
	 *                      <b>{@code LONGVARCHAR}</b> value. The bold SQL type
	 *                      designates the recommended return type.
	 */
	public String getString(String columnName) throws SQLException {
		return getString(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>boolean</code> value.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return the column value as a <code>boolean</code> in the Java programming
	 *         language; if the value is SQL <code>NULL</code>, the result is
	 *         <code>false</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>BOOLEAN</code> value
	 * @see #getBoolean(int)
	 */
	public boolean getBoolean(String columnName) throws SQLException {
		return getBoolean(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>byte</code> value.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return the column value as a <code>byte</code> in the Java programming
	 *         language; if the value is SQL <code>NULL</code>, the result is
	 *         <code>0</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL
	 *                      <code><B>TINYINT</B>, SMALLINT, INTEGER,
	 * BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, CHAR,
	 * VARCHAR</code>    or <code>LONGVARCHAR</code> value. The bold type
	 *                      designates the recommended return type
	 */
	public byte getByte(String columnName) throws SQLException {
		return getByte(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>short</code> value.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>0</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL
	 *                      <code>TINYINT, <b>SMALLINT</b>, INTEGER
	 * BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, CHAR,
	 * VARCHAR</code>    or <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type.
	 * @see #getShort(int)
	 */
	public short getShort(String columnName) throws SQLException {
		return getShort(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value stored in the designated column of the current row as an
	 * <code>int</code> value.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>0</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL
	 *                      <code>TINYINT, SMALLINT, <b>INTEGER</b>, BIGINT, REAL
	 * FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, CHAR, VARCHAR</code> or
	 *                      <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type.
	 */
	public int getInt(String columnName) throws SQLException {
		return getInt(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>long</code> value.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>0</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL
	 *                      <code>TINYINT, SMALLINT, INTEGER
	 * <b>BIGINT</b>, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, CHAR,
	 * VARCHAR</code>    or <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type.
	 * @see #getLong(int)
	 */
	public long getLong(String columnName) throws SQLException {
		return getLong(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>float</code> value.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>0</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL
	 *                      <code>TINYINT, SMALLINT, INTEGER
	 * BIGINT, <b>REAL</b>, FLOAT, DOUBLE, DECIMAL, NUMERIC, BIT, CHAR,
	 * VARCHAR</code>    or <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type.
	 * @see #getFloat(String)
	 */
	public float getFloat(String columnName) throws SQLException {
		return getFloat(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value stored in the designated column of the current row of
	 * this <code>CachedRowSetImpl</code> object as a <code>double</code> value.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>0</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL
	 *                      <code>TINYINT, SMALLINT, INTEGER
	 * BIGINT, REAL, <b>FLOAT</b>, <b>DOUBLE</b>, DECIMAL, NUMERIC, BIT, CHAR,
	 * VARCHAR</code>    or <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return types.
	 * @see #getDouble(int)
	 */
	public double getDouble(String columnName) throws SQLException {
		return getDouble(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>java.math.BigDecimal</code> object.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @param scale      the number of digits to the right of the decimal point
	 * @return a java.math.BugDecimal object with <code><i>scale</i></code> number
	 *         of digits to the right of the decimal point.
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL
	 *                      <code>TINYINT, SMALLINT, INTEGER
	 * BIGINT, REAL, FLOAT, DOUBLE, <b>DECIMAL</b>, <b>NUMERIC</b>, BIT CHAR,
	 * VARCHAR</code>    or <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type that this method
	 *                      is used to retrieve.
	 * @deprecated Use the <code>getBigDecimal(String columnName)</code> method
	 *             instead
	 */
	@Override
	public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
		return getBigDecimal(getColIdxByName(columnName), scale);
	}

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>byte</code> array. The bytes represent the raw values returned by the
	 * driver.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return the column value as a <code>byte</code> array in the Java programming
	 *         language; if the value is SQL <code>NULL</code>, the result is
	 *         <code>null</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL
	 *                      <code><b>BINARY</b>, <b>VARBINARY</b>
	 * </code>           or <code>LONGVARBINARY</code> values The bold SQL type
	 *                      designates the recommended return type.
	 * @see #getBytes(int)
	 */
	public byte[] getBytes(String columnName) throws SQLException {
		return getBytes(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>java.sql.Date</code> object.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>DATE</code> or
	 *                      <code>TIMESTAMP</code> value
	 */
	public java.sql.Date getDate(String columnName) throws SQLException {
		return getDate(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>java.sql.Time</code> object.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if the given column name does not match one of this
	 *                      rowset's column names or the cursor is not on one of
	 *                      this rowset's rows or its insert row
	 */
	public java.sql.Time getTime(String columnName) throws SQLException {
		return getTime(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>java.sql.Timestamp</code> object.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if the given column name does not match one of this
	 *                      rowset's column names or the cursor is not on one of
	 *                      this rowset's rows or its insert row
	 */
	public java.sql.Timestamp getTimestamp(String columnName) throws SQLException {
		return getTimestamp(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>InputStream</code> object.
	 *
	 * A column value can be retrieved as a stream of ASCII characters and then read
	 * in chunks from the stream. This method is particularly suitable for
	 * retrieving large <code>LONGVARCHAR</code> values. The
	 * <code>SyncProvider</code> will rely on the JDBC driver to do any necessary
	 * conversion from the database format into ASCII format.
	 *
	 * <P>
	 * <B>Note:</B> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a <code>getXXX</code>
	 * method implicitly closes the stream.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of one-byte ASCII characters. If the value is SQL
	 *         <code>NULL</code>, the result is <code>null</code>.
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>CHAR, VARCHAR</code>,
	 *                      <code><b>LONGVARCHAR</b></code>
	 *                      <code>BINARY, VARBINARY</code> or
	 *                      <code>LONGVARBINARY</code> value. The bold SQL type
	 *                      designates the recommended return types that this method
	 *                      is used to retrieve.
	 * @see #getAsciiStream(int)
	 */
	public InputStream getAsciiStream(String columnName) throws SQLException {
		return getAsciiStream(getColIdxByName(columnName));

	}

	/**
	 * A column value can be retrieved as a stream of Unicode characters and then
	 * read in chunks from the stream. This method is particularly suitable for
	 * retrieving large <code>LONGVARCHAR</code> values. The JDBC driver will do any
	 * necessary conversion from the database format into Unicode.
	 *
	 * <P>
	 * <B>Note:</B> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a <code>getXXX</code>
	 * method implicitly closes the stream.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of two-byte Unicode characters. If the value is SQL
	 *         <code>NULL</code>, the result is <code>null</code>.
	 * @throws SQLException if the given column name does not match one of this
	 *                      rowset's column names or the cursor is not on one of
	 *                      this rowset's rows or its insert row
	 * @deprecated use the method <code>getCharacterStream</code> instead
	 */
	@Override
	public InputStream getUnicodeStream(String columnName) throws SQLException {
		return getUnicodeStream(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>InputStream</code> object.
	 * <P>
	 * A column value can be retrieved as a stream of uninterpreted bytes and then
	 * read in chunks from the stream. This method is particularly suitable for
	 * retrieving large <code>LONGVARBINARY</code> values.
	 *
	 * <P>
	 * <B>Note:</B> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a get method
	 * implicitly closes the stream. Also, a stream may return <code>0</code> for
	 * <code>CachedRowSetImpl.available()</code> whether there is data available or
	 * not.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of uninterpreted bytes. If the value is SQL <code>NULL</code>,
	 *         the result is <code>null</code>.
	 * @throws SQLException if (1) the given column name is unknown, (2) the cursor
	 *                      is not on one of this rowset's rows or its insert row,
	 *                      or (3) the designated column does not store an SQL
	 *                      <code>BINARY, VARBINARY</code> or
	 *                      <code><b>LONGVARBINARY</b></code> The bold type
	 *                      indicates the SQL type that this method is recommened to
	 *                      retrieve.
	 * @see #getBinaryStream(int)
	 *
	 */
	public InputStream getBinaryStream(String columnName) throws SQLException {
		return getBinaryStream(getColIdxByName(columnName));
	}

	// Advanced features:

	/**
	 * The first warning reported by calls on this <code>CachedRowSetImpl</code>
	 * object is returned. Subsequent <code>CachedRowSetImpl</code> warnings will be
	 * chained to this <code>SQLWarning</code>.
	 *
	 * <P>
	 * The warning chain is automatically cleared each time a new row is read.
	 *
	 * <P>
	 * <B>Note:</B> This warning chain only covers warnings caused by
	 * <code>ResultSet</code> methods. Any warning caused by statement methods (such
	 * as reading OUT parameters) will be chained on the <code>Statement</code>
	 * object.
	 *
	 * @return the first SQLWarning or null
	 */
	public SQLWarning getWarnings() {
		return sqlwarn;
	}

	/**
	 * Clears all the warnings reporeted for the <code>CachedRowSetImpl</code>
	 * object. After a call to this method, the <code>getWarnings</code> method
	 * returns <code>null</code> until a new warning is reported for this
	 * <code>CachedRowSetImpl</code> object.
	 */
	public void clearWarnings() {
		sqlwarn = null;
	}

	/**
	 * Retrieves the name of the SQL cursor used by this
	 * <code>CachedRowSetImpl</code> object.
	 *
	 * <P>
	 * In SQL, a result table is retrieved through a cursor that is named. The
	 * current row of a <code>ResultSet</code> can be updated or deleted using a
	 * positioned update/delete statement that references the cursor name. To ensure
	 * that the cursor has the proper isolation level to support an update
	 * operation, the cursor's <code>SELECT</code> statement should be of the form
	 * <code>select for update</code>. If the <code>for update</code> clause is
	 * omitted, positioned updates may fail.
	 *
	 * <P>
	 * JDBC supports this SQL feature by providing the name of the SQL cursor used
	 * by a <code>ResultSet</code> object. The current row of a result set is also
	 * the current row of this SQL cursor.
	 *
	 * <P>
	 * <B>Note:</B> If positioned updates are not supported, an
	 * <code>SQLException</code> is thrown.
	 *
	 * @return the SQL cursor name for this <code>CachedRowSetImpl</code> object's
	 *         cursor
	 * @throws SQLException if an error occurs
	 */
	public String getCursorName() throws SQLException {
		throw new SQLException("Positioned updates not supported");
	}

	/**
	 * Retrieves a <code>ResultSetMetaData</code> object instance that contains
	 * information about the <code>CachedRowSet</code> object. However, applications
	 * should cast the returned object to a <code>RowSetMetaData</code> interface
	 * implementation. In the reference implementation, this cast can be done on the
	 * <code>RowSetMetaDataImpl</code> class.
	 * <P>
	 * For example:
	 * 
	 * <pre>
	 * CachedRowSet crs = new CachedRowSetImpl();
	 * RowSetMetaDataImpl metaData = (RowSetMetaDataImpl) crs.getMetaData();
	 * // Set the number of columns in the RowSet object for
	 * // which this RowSetMetaDataImpl object was created to the
	 * // given number.
	 * metaData.setColumnCount(3);
	 * crs.setMetaData(metaData);
	 * </pre>
	 *
	 * @return the <code>ResultSetMetaData</code> object that describes this
	 *         <code>CachedRowSetImpl</code> object's columns
	 * @throws SQLException if an error occurs in generating the RowSet meta data;
	 *                      or if the <code>CachedRowSetImpl</code> is empty.
	 * @see javax.sql.RowSetMetaData
	 */
	public ResultSetMetaData getMetaData() throws SQLException {
		return rowSetMetaData;
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as an <code>Object</code> value.
	 * <P>
	 * The type of the <code>Object</code> will be the default Java object type
	 * corresponding to the column's SQL type, following the mapping for built-in
	 * types specified in the JDBC 3.0 specification.
	 * <P>
	 * This method may also be used to read datatabase-specific abstract data types.
	 * <P>
	 * This implementation of the method <code>getObject</code> extends its behavior
	 * so that it gets the attributes of an SQL structured type as an array of
	 * <code>Object</code> values. This method also custom maps SQL user-defined
	 * types to classes in the Java programming language. When the specified column
	 * contains a structured or distinct value, the behavior of this method is as if
	 * it were a call to the method <code>getObject(columnIndex,
	 * this.getStatement().getConnection().getTypeMap())</code>.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return a <code>java.lang.Object</code> holding the column value; if the
	 *         value is SQL <code>NULL</code>, the result is <code>null</code>
	 * @throws SQLException if the given column index is out of bounds, the cursor
	 *                      is not on a valid row, or there is a problem getting the
	 *                      <code>Class</code> object for a custom mapping
	 * @see #getObject(String)
	 */
	public Object getObject(int columnIndex) throws SQLException {
		return getValue(columnIndex, null, value -> {
			if (value instanceof Struct) {
				Struct s = (Struct) value;
				Map<String, Class<?>> map = getTypeMap();
				// look up the class in the map
				Class<?> c = map.get(s.getSQLTypeName());
				if (c != null) {
					// create new instance of the class
					SQLData obj = null;
					try {
						@SuppressWarnings("deprecation")
						Object tmp = c.newInstance();
						obj = (SQLData) tmp;
					} catch (Exception ex) {
						throw new SQLException("Unable to Instantiate: ", ex);
					}
					// get the attributes from the struct
					Object[] attribs = s.getAttributes(map);
					// create the SQLInput "stream"
					SQLInputImpl sqlInput = new SQLInputImpl(attribs, map);
					// read the values...
					obj.readSQL(sqlInput, s.getSQLTypeName());
					return obj;
				}
			}
			return value;
		});
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as an <code>Object</code> value.
	 * <P>
	 * The type of the <code>Object</code> will be the default Java object type
	 * corresponding to the column's SQL type, following the mapping for built-in
	 * types specified in the JDBC 3.0 specification.
	 * <P>
	 * This method may also be used to read datatabase-specific abstract data types.
	 * <P>
	 * This implementation of the method <code>getObject</code> extends its behavior
	 * so that it gets the attributes of an SQL structured type as an array of
	 * <code>Object</code> values. This method also custom maps SQL user-defined
	 * types to classes in the Java programming language. When the specified column
	 * contains a structured or distinct value, the behavior of this method is as if
	 * it were a call to the method <code>getObject(columnIndex,
	 * this.getStatement().getConnection().getTypeMap())</code>.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @return a <code>java.lang.Object</code> holding the column value; if the
	 *         value is SQL <code>NULL</code>, the result is <code>null</code>
	 * @throws SQLException if (1) the given column name does not match one of this
	 *                      rowset's column names, (2) the cursor is not on a valid
	 *                      row, or (3) there is a problem getting the
	 *                      <code>Class</code> object for a custom mapping
	 * @see #getObject(int)
	 */
	public Object getObject(String columnName) throws SQLException {
		return getObject(getColIdxByName(columnName));
	}

	// ----------------------------------------------------------------

	/**
	 * Maps the given column name for one of this <code>CachedRowSetImpl</code>
	 * object's columns to its column number.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @return the column index of the given column name
	 * @throws SQLException if the given column name does not match one of this
	 *                      rowset's column names
	 */
	public int findColumn(String columnName) throws SQLException {
		return getColIdxByName(columnName);
	}

	// --------------------------JDBC 2.0-----------------------------------

	// ---------------------------------------------------------------------
	// Getter's and Setter's
	// ---------------------------------------------------------------------

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>Reader</code> object.
	 *
	 * <P>
	 * <B>Note:</B> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a <code>getXXX</code>
	 * method implicitly closes the stream.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return a Java character stream that delivers the database column value as a
	 *         stream of two-byte unicode characters in a <code>Reader</code>
	 *         object. If the value is SQL <code>NULL</code>, the result is
	 *         <code>null</code>.
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>CHAR, VARCHAR, <b>LONGVARCHAR</b>, BINARY, VARBINARY</code>
	 *                      or <code>LONGVARBINARY</code> value. The bold SQL type
	 *                      designates the recommended return type.
	 * @see #getCharacterStream(String)
	 */
	public Reader getCharacterStream(int columnIndex) throws SQLException {

		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (isBinary(rowSetMetaData.getColumnType(columnIndex))) {
			Object value = getCurrentRow().getColumnObject(columnIndex);
			if (value == null) {
				lastValueNull = true;
				return null;
			}
			charStream = new InputStreamReader(new ByteArrayInputStream((byte[]) value));
		} else if (isString(rowSetMetaData.getColumnType(columnIndex))) {
			Object value = getCurrentRow().getColumnObject(columnIndex);
			if (value == null) {
				lastValueNull = true;
				return null;
			}
			charStream = new StringReader(value.toString());
		} else {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		return charStream;
	}

	/**
	 * Retrieves the value stored in the designated column of the current row as a
	 * <code>Reader</code> object.
	 *
	 * <P>
	 * <B>Note:</B> All the data in the returned stream must be read prior to
	 * getting the value of any other column. The next call to a <code>getXXX</code>
	 * method implicitly closes the stream.
	 *
	 * @param columnName a <code>String</code> object giving the SQL name of a
	 *                   column in this <code>CachedRowSetImpl</code> object
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of two-byte Unicode characters. If the value is SQL
	 *         <code>NULL</code>, the result is <code>null</code>.
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL
	 *                      <code>CHAR, VARCHAR, <b>LONGVARCHAR</b>,
	 * BINARY, VARYBINARY</code> or <code>LONGVARBINARY</code> value. The bold SQL
	 *                      type designates the recommended return type.
	 */
	public Reader getCharacterStream(String columnName) throws SQLException {
		return getCharacterStream(getColIdxByName(columnName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>java.math.BigDecimal</code>
	 * object.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @return a <code>java.math.BigDecimal</code> value with full precision; if the
	 *         value is SQL <code>NULL</code>, the result is <code>null</code>
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>TINYINT, SMALLINT, INTEGER, BIGINT, REAL,
	 * FLOAT, DOUBLE, <b>DECIMAL</b>, <b>NUMERIC</b>, BIT, CHAR, VARCHAR</code> or
	 *                      <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return types that this method
	 *                      is used to retrieve.
	 * @see #getBigDecimal(String)
	 */
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return getValue(columnIndex, null, value -> {
			try {
				return (new BigDecimal(value.toString().trim()));
			} catch (NumberFormatException ex) {
				throw new SQLException(failMessage("getBigDecimal", value, columnIndex));
			}
		});
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>java.math.BigDecimal</code>
	 * object.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @return a <code>java.math.BigDecimal</code> value with full precision; if the
	 *         value is SQL <code>NULL</code>, the result is <code>null</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL
	 *                      <code>TINYINT, SMALLINT, INTEGER
	 * BIGINT, REAL, FLOAT, DOUBLE, <b>DECIMAL</b>, <b>NUMERIC</b>, BIT CHAR,
	 * VARCHAR</code>    or <code>LONGVARCHAR</code> value. The bold SQL type
	 *                      designates the recommended return type that this method
	 *                      is used to retrieve
	 * @see #getBigDecimal(int)
	 */
	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		return getBigDecimal(getColIdxByName(columnName));
	}

	// ---------------------------------------------------------------------
	// Traversal/Positioning
	// ---------------------------------------------------------------------

	/**
	 * Returns the number of rows in this <code>CachedRowSetImpl</code> object.
	 *
	 * @return number of rows in the rowset
	 */
	public int size() {
		return numRows;
	}

	/**
	 * Indicates whether the cursor is before the first row in this
	 * <code>CachedRowSetImpl</code> object.
	 *
	 * @return <code>true</code> if the cursor is before the first row;
	 *         <code>false</code> otherwise or if the rowset contains no rows
	 * @throws SQLException if an error occurs
	 */
	public boolean isBeforeFirst() throws SQLException {
		return cursorPos == 0 && numRows > 0;
	}

	/**
	 * Indicates whether the cursor is after the last row in this
	 * <code>CachedRowSetImpl</code> object.
	 *
	 * @return <code>true</code> if the cursor is after the last row;
	 *         <code>false</code> otherwise or if the rowset contains no rows
	 * @throws SQLException if an error occurs
	 */
	public boolean isAfterLast() throws SQLException {
		return cursorPos == numRows + 1 && numRows > 0;
	}

	/**
	 * Indicates whether the cursor is on the first row in this
	 * <code>CachedRowSetImpl</code> object.
	 *
	 * @return <code>true</code> if the cursor is on the first row;
	 *         <code>false</code> otherwise or if the rowset contains no rows
	 * @throws SQLException if an error occurs
	 */
	public boolean isFirst() throws SQLException {
		// this becomes nasty because of deletes.
		int saveCursorPos = cursorPos;
		int saveAbsoluteCursorPos = absolutePos;
		internalFirst();
		if (cursorPos == saveCursorPos) {
			return true;
		} else {
			cursorPos = saveCursorPos;
			absolutePos = saveAbsoluteCursorPos;
			return false;
		}
	}

	/**
	 * Indicates whether the cursor is on the last row in this
	 * <code>CachedRowSetImpl</code> object.
	 * <P>
	 * Note: Calling the method <code>isLast</code> may be expensive because the
	 * JDBC driver might need to fetch ahead one row in order to determine whether
	 * the current row is the last row in this rowset.
	 *
	 * @return <code>true</code> if the cursor is on the last row;
	 *         <code>false</code> otherwise or if this rowset contains no rows
	 * @throws SQLException if an error occurs
	 */
	public boolean isLast() throws SQLException {
		int saveCursorPos = cursorPos;
		int saveAbsoluteCursorPos = absolutePos;
		boolean saveShowDeleted = getShowDeleted();
		setShowDeleted(true);
		internalLast();
		if (cursorPos == saveCursorPos) {
			setShowDeleted(saveShowDeleted);
			return true;
		} else {
			setShowDeleted(saveShowDeleted);
			cursorPos = saveCursorPos;
			absolutePos = saveAbsoluteCursorPos;
			return false;
		}
	}

	/**
	 * Moves this <code>CachedRowSetImpl</code> object's cursor to the front of the
	 * rowset, just before the first row. This method has no effect if this rowset
	 * contains no rows.
	 *
	 * @throws SQLException if an error occurs or the type of this rowset is
	 *                      <code>ResultSet.TYPE_FORWARD_ONLY</code>
	 */
	public void beforeFirst() throws SQLException {
		if (getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException("beforeFirst: Invalid cursor operation");
		}
		cursorPos = 0;
		absolutePos = 0;
		notifyCursorMoved();
	}

	/**
	 * Moves this <code>CachedRowSetImpl</code> object's cursor to the end of the
	 * rowset, just after the last row. This method has no effect if this rowset
	 * contains no rows.
	 *
	 * @throws SQLException if an error occurs
	 */
	public void afterLast() throws SQLException {
		if (numRows > 0) {
			cursorPos = numRows + 1;
			absolutePos = 0;
			notifyCursorMoved();
		}
	}

	/**
	 * Moves this <code>CachedRowSetImpl</code> object's cursor to the first row and
	 * returns <code>true</code> if the operation was successful. This method also
	 * notifies registered listeners that the cursor has moved.
	 *
	 * @return <code>true</code> if the cursor is on a valid row; <code>false</code>
	 *         otherwise or if there are no rows in this
	 *         <code>CachedRowSetImpl</code> object
	 * @throws SQLException if the type of this rowset is
	 *                      <code>ResultSet.TYPE_FORWARD_ONLY</code>
	 */
	public boolean first() throws SQLException {
		if (getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException("First: Invalid cursor operation");
		}

		// move and notify
		boolean ret = this.internalFirst();
		notifyCursorMoved();

		return ret;
	}

	/**
	 * Moves this <code>CachedRowSetImpl</code> object's cursor to the first row and
	 * returns <code>true</code> if the operation is successful.
	 * <P>
	 * This method is called internally by the methods <code>first</code>,
	 * <code>isFirst</code>, and <code>absolute</code>. It in turn calls the method
	 * <code>internalNext</code> in order to handle the case where the first row is
	 * a deleted row that is not visible.
	 * <p>
	 * This is a implementation only method and is not required as a standard
	 * implementation of the <code>CachedRowSet</code> interface.
	 *
	 * @return <code>true</code> if the cursor moved to the first row;
	 *         <code>false</code> otherwise
	 * @throws SQLException if an error occurs
	 */
	protected boolean internalFirst() throws SQLException {
		boolean ret = false;

		if (numRows > 0) {
			cursorPos = 1;
			if (!getShowDeleted() && rowDeleted()) {
				ret = internalNext();
			} else {
				ret = true;
			}
		}

		if (ret)
			absolutePos = 1;
		else
			absolutePos = 0;

		return ret;
	}

	/**
	 * Moves this <code>CachedRowSetImpl</code> object's cursor to the last row and
	 * returns <code>true</code> if the operation was successful. This method also
	 * notifies registered listeners that the cursor has moved.
	 *
	 * @return <code>true</code> if the cursor is on a valid row; <code>false</code>
	 *         otherwise or if there are no rows in this
	 *         <code>CachedRowSetImpl</code> object
	 * @throws SQLException if the type of this rowset is
	 *                      <code>ResultSet.TYPE_FORWARD_ONLY</code>
	 */
	public boolean last() throws SQLException {
		if (getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException("last: TYPE_FORWARD_ONLY");
		}

		// move and notify
		boolean ret = this.internalLast();
		notifyCursorMoved();

		return ret;
	}

	/**
	 * Moves this <code>CachedRowSetImpl</code> object's cursor to the last row and
	 * returns <code>true</code> if the operation is successful.
	 * <P>
	 * This method is called internally by the method <code>last</code> when rows
	 * have been deleted and the deletions are not visible. The method
	 * <code>internalLast</code> handles the case where the last row is a deleted
	 * row that is not visible by in turn calling the method
	 * <code>internalPrevious</code>.
	 * <p>
	 * This is a implementation only method and is not required as a standard
	 * implementation of the <code>CachedRowSet</code> interface.
	 *
	 * @return <code>true</code> if the cursor moved to the last row;
	 *         <code>false</code> otherwise
	 * @throws SQLException if an error occurs
	 */
	protected boolean internalLast() throws SQLException {
		boolean ret = false;

		if (numRows > 0) {
			cursorPos = numRows;
			if (!getShowDeleted() && rowDeleted()) {
				ret = internalPrevious();
			} else {
				ret = true;
			}
		}
		if (ret)
			absolutePos = numRows - numDeleted;
		else
			absolutePos = 0;
		return ret;
	}

	/**
	 * Returns the number of the current row in this <code>CachedRowSetImpl</code>
	 * object. The first row is number 1, the second number 2, and so on.
	 *
	 * @return the number of the current row; <code>0</code> if there is no current
	 *         row
	 * @throws SQLException if an error occurs; or if the
	 *                      <code>CacheRowSetImpl</code> is empty
	 */
	public int getRow() throws SQLException {
		// are we on a valid row? Valid rows are between first and last
		if (numRows > 0 && cursorPos > 0 && cursorPos < (numRows + 1) && (!getShowDeleted() && !rowDeleted())) {
			return absolutePos;
		} else if (getShowDeleted()) {
			return cursorPos;
		} else {
			return 0;
		}
	}

	/**
	 * Moves this <code>CachedRowSetImpl</code> object's cursor to the row number
	 * specified.
	 *
	 * <p>
	 * If the number is positive, the cursor moves to an absolute row with respect
	 * to the beginning of the rowset. The first row is row 1, the second is row 2,
	 * and so on. For example, the following command, in which <code>crs</code> is a
	 * <code>CachedRowSetImpl</code> object, moves the cursor to the fourth row,
	 * starting from the beginning of the rowset.
	 * 
	 * <PRE>
	 * <code>
	 *
	 *    crs.absolute(4);
	 *
	 * </code>
	 * </PRE>
	 * <P>
	 * If the number is negative, the cursor moves to an absolute row position with
	 * respect to the end of the rowset. For example, calling
	 * <code>absolute(-1)</code> positions the cursor on the last row,
	 * <code>absolute(-2)</code> moves it on the next-to-last row, and so on. If the
	 * <code>CachedRowSetImpl</code> object <code>crs</code> has five rows, the
	 * following command moves the cursor to the fourth-to-last row, which in the
	 * case of a rowset with five rows, is also the second row, counting from the
	 * beginning.
	 * 
	 * <PRE>
	 * <code>
	 *
	 *    crs.absolute(-4);
	 *
	 * </code>
	 * </PRE>
	 *
	 * If the number specified is larger than the number of rows, the cursor will
	 * move to the position after the last row. If the number specified would move
	 * the cursor one or more rows before the first row, the cursor moves to the
	 * position before the first row.
	 * <P>
	 * Note: Calling <code>absolute(1)</code> is the same as calling the method
	 * <code>first()</code>. Calling <code>absolute(-1)</code> is the same as
	 * calling <code>last()</code>.
	 *
	 * @param row a positive number to indicate the row, starting row numbering from
	 *            the first row, which is <code>1</code>; a negative number to
	 *            indicate the row, starting row numbering from the last row, which
	 *            is <code>-1</code>; it must not be <code>0</code>
	 * @return <code>true</code> if the cursor is on the rowset; <code>false</code>
	 *         otherwise
	 * @throws SQLException if the given cursor position is <code>0</code> or the
	 *                      type of this rowset is
	 *                      <code>ResultSet.TYPE_FORWARD_ONLY</code>
	 */
	public boolean absolute(int row) throws SQLException {
		if (row == 0 || getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException("absolute: " + MESSAGE_INVALID_CURSOR_POSITION);
		}

		if (row > 0) { // we are moving foward
			if (row > numRows) {
				// fell off the end
				afterLast();
				return false;
			} else {
				if (absolutePos <= 0)
					internalFirst();
			}
		} else { // we are moving backward
			if (cursorPos + row < 0) {
				// fell off the front
				beforeFirst();
				return false;
			} else {
				if (absolutePos >= 0)
					internalLast();
			}
		}

		// Now move towards the absolute row that we're looking for
		while (absolutePos != row) {
			if (absolutePos < row) {
				if (!internalNext())
					break;
			} else {
				if (!internalPrevious())
					break;
			}
		}

		notifyCursorMoved();

		return !isAfterLast() && !isBeforeFirst();
	}

	/**
	 * Moves the cursor the specified number of rows from the current position, with
	 * a positive number moving it forward and a negative number moving it backward.
	 * <P>
	 * If the number is positive, the cursor moves the specified number of rows
	 * toward the end of the rowset, starting at the current row. For example, the
	 * following command, in which <code>crs</code> is a
	 * <code>CachedRowSetImpl</code> object with 100 rows, moves the cursor forward
	 * four rows from the current row. If the current row is 50, the cursor would
	 * move to row 54.
	 * 
	 * <PRE>
	 * <code>
	 *
	 *    crs.relative(4);
	 *
	 * </code>
	 * </PRE>
	 * <P>
	 * If the number is negative, the cursor moves back toward the beginning the
	 * specified number of rows, starting at the current row. For example, calling
	 * the method <code>absolute(-1)</code> positions the cursor on the last row,
	 * <code>absolute(-2)</code> moves it on the next-to-last row, and so on. If the
	 * <code>CachedRowSetImpl</code> object <code>crs</code> has five rows, the
	 * following command moves the cursor to the fourth-to-last row, which in the
	 * case of a rowset with five rows, is also the second row from the beginning.
	 * 
	 * <PRE>
	 * <code>
	 *
	 *    crs.absolute(-4);
	 *
	 * </code>
	 * </PRE>
	 *
	 * If the number specified is larger than the number of rows, the cursor will
	 * move to the position after the last row. If the number specified would move
	 * the cursor one or more rows before the first row, the cursor moves to the
	 * position before the first row. In both cases, this method throws an
	 * <code>SQLException</code>.
	 * <P>
	 * Note: Calling <code>absolute(1)</code> is the same as calling the method
	 * <code>first()</code>. Calling <code>absolute(-1)</code> is the same as
	 * calling <code>last()</code>. Calling <code>relative(0)</code> is valid, but
	 * it does not change the cursor position.
	 *
	 * @param rows an <code>int</code> indicating the number of rows to move the
	 *             cursor, starting at the current row; a positive number moves the
	 *             cursor forward; a negative number moves the cursor backward; must
	 *             not move the cursor past the valid rows
	 * @return <code>true</code> if the cursor is on a row in this
	 *         <code>CachedRowSetImpl</code> object; <code>false</code> otherwise
	 * @throws SQLException if there are no rows in this rowset, the cursor is
	 *                      positioned either before the first row or after the last
	 *                      row, or the rowset is type
	 *                      <code>ResultSet.TYPE_FORWARD_ONLY</code>
	 */
	public boolean relative(int rows) throws SQLException {
		if (numRows == 0 || isBeforeFirst() || isAfterLast() || getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException("relative: " + MESSAGE_INVALID_CURSOR_POSITION);
		}

		if (rows == 0) {
			return true;
		}

		if (rows > 0) { // we are moving forward
			if (cursorPos + rows > numRows) {
				// fell off the end
				afterLast();
			} else {
				for (int i = 0; i < rows; i++) {
					if (!internalNext())
						break;
				}
			}
		} else { // we are moving backward
			if (cursorPos + rows < 0) {
				// fell off the front
				beforeFirst();
			} else {
				for (int i = rows; i < 0; i++) {
					if (!internalPrevious())
						break;
				}
			}
		}
		notifyCursorMoved();
		return !isAfterLast() && !isBeforeFirst();
	}

	/**
	 * Moves this <code>CachedRowSetImpl</code> object's cursor to the previous row
	 * and returns <code>true</code> if the cursor is on a valid row or
	 * <code>false</code> if it is not. This method also notifies all listeners
	 * registered with this <code>CachedRowSetImpl</code> object that its cursor has
	 * moved.
	 * <P>
	 * Note: calling the method <code>previous()</code> is not the same as calling
	 * the method <code>relative(-1)</code>. This is true because it is possible to
	 * call <code>previous()</code> from the insert row, from after the last row, or
	 * from the current row, whereas <code>relative</code> may only be called from
	 * the current row.
	 * <P>
	 * The method <code>previous</code> may used in a <code>while</code> loop to
	 * iterate through a rowset starting after the last row and moving toward the
	 * beginning. The loop ends when <code>previous</code> returns
	 * <code>false</code>, meaning that there are no more rows. For example, the
	 * following code fragment retrieves all the data in the
	 * <code>CachedRowSetImpl</code> object <code>crs</code>, which has three
	 * columns. Note that the cursor must initially be positioned after the last row
	 * so that the first call to the method <code>previous</code> places the cursor
	 * on the last line.
	 * 
	 * <PRE>
	 *  <code>
	 *
	 *     crs.afterLast();
	 *     while (previous()) {
	 *         String name = crs.getString(1);
	 *         int age = crs.getInt(2);
	 *         short ssn = crs.getShort(3);
	 *         System.out.println(name + "   " + age + "   " + ssn);
	 *     }
	 *
	 * </code>
	 * </PRE>
	 * 
	 * This method throws an <code>SQLException</code> if the cursor is not on a row
	 * in the rowset, before the first row, or after the last row.
	 *
	 * @return <code>true</code> if the cursor is on a valid row; <code>false</code>
	 *         if it is before the first row or after the last row
	 * @throws SQLException if the cursor is not on a valid position or the type of
	 *                      this rowset is <code>ResultSet.TYPE_FORWARD_ONLY</code>
	 */
	public boolean previous() throws SQLException {
		if (getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException("last: TYPE_FORWARD_ONLY");
		}
		/*
		 * make sure things look sane. The cursor must be positioned in the rowset or
		 * before first (0) or after last (numRows + 1)
		 */
		if (cursorPos < 0 || cursorPos > numRows + 1) {
			throw new SQLException(MESSAGE_INVALID_CURSOR_POSITION);
		}
		// move and notify
		boolean ret = this.internalPrevious();
		notifyCursorMoved();

		return ret;
	}

	/**
	 * Moves the cursor to the previous row in this <code>CachedRowSetImpl</code>
	 * object, skipping past deleted rows that are not visible; returns
	 * <code>true</code> if the cursor is on a row in this rowset and
	 * <code>false</code> when the cursor goes before the first row.
	 * <P>
	 * This method is called internally by the method <code>previous</code>.
	 * <P>
	 * This is a implementation only method and is not required as a standard
	 * implementation of the <code>CachedRowSet</code> interface.
	 *
	 * @return <code>true</code> if the cursor is on a row in this rowset;
	 *         <code>false</code> when the cursor reaches the position before the
	 *         first row
	 * @throws SQLException if an error occurs
	 */
	protected boolean internalPrevious() throws SQLException {
		boolean ret = false;

		do {
			if (cursorPos > 1) {
				--cursorPos;
				ret = true;
			} else if (cursorPos == 1) {
				// decrement to before first
				--cursorPos;
				ret = false;
				break;
			}
		} while (!getShowDeleted() && rowDeleted());

		/*
		 * Each call to internalPrevious may move the cursor over multiple rows, the
		 * absolute position moves one row
		 */
		if (ret)
			--absolutePos;
		else
			absolutePos = 0;

		return ret;
	}

	// ---------------------------------------------------------------------
	// Updates
	// ---------------------------------------------------------------------

	/**
	 * Indicates whether the current row of this <code>CachedRowSetImpl</code>
	 * object has been updated. The value returned depends on whether this rowset
	 * can detect updates: <code>false</code> will always be returned if it does not
	 * detect updates.
	 *
	 * @return <code>true</code> if the row has been visibly updated by the owner or
	 *         another and updates are detected; <code>false</code> otherwise
	 * @throws SQLException if the cursor is on the insert row or not not on a valid
	 *                      row
	 *
	 * @see DatabaseMetaData#updatesAreDetected
	 */
	public boolean rowUpdated() throws SQLException {
		// make sure the cursor is on a valid row
		checkCursor();
		if (onInsertRow) {
			throw new SQLException(MESSAGE_INVALID_OPERATION);
		}
		return (((Row) getCurrentRow()).getUpdated());
	}

	/**
	 * Indicates whether the designated column of the current row of this
	 * <code>CachedRowSetImpl</code> object has been updated. The value returned
	 * depends on whether this rowset can detcted updates: <code>false</code> will
	 * always be returned if it does not detect updates.
	 *
	 * @param idx the index identifier of the column that may be have been updated.
	 * @return <code>true</code> is the designated column has been updated and the
	 *         rowset detects updates; <code>false</code> if the rowset has not been
	 *         updated or the rowset does not detect updates
	 * @throws SQLException if the cursor is on the insert row or not on a valid row
	 * @see DatabaseMetaData#updatesAreDetected
	 */
	public boolean columnUpdated(int idx) throws SQLException {
		// make sure the cursor is on a valid row
		checkCursor();
		if (onInsertRow) {
			throw new SQLException(MESSAGE_INVALID_OPERATION);
		}
		return (((Row) getCurrentRow()).getColUpdated(idx - 1));
	}

	/**
	 * Indicates whether the designated column of the current row of this
	 * <code>CachedRowSetImpl</code> object has been updated. The value returned
	 * depends on whether this rowset can detcted updates: <code>false</code> will
	 * always be returned if it does not detect updates.
	 *
	 * @param columnName the <code>String</code> column name column that may be have
	 *                   been updated.
	 * @return <code>true</code> is the designated column has been updated and the
	 *         rowset detects updates; <code>false</code> if the rowset has not been
	 *         updated or the rowset does not detect updates
	 * @throws SQLException if the cursor is on the insert row or not on a valid row
	 * @see DatabaseMetaData#updatesAreDetected
	 */
	public boolean columnUpdated(String columnName) throws SQLException {
		return columnUpdated(getColIdxByName(columnName));
	}

	/**
	 * Indicates whether the current row has been inserted. The value returned
	 * depends on whether or not the rowset can detect visible inserts.
	 *
	 * @return <code>true</code> if a row has been inserted and inserts are
	 *         detected; <code>false</code> otherwise
	 * @throws SQLException if the cursor is on the insert row or not not on a valid
	 *                      row
	 *
	 * @see DatabaseMetaData#insertsAreDetected
	 */
	public boolean rowInserted() throws SQLException {
		// make sure the cursor is on a valid row
		checkCursor();
		if (onInsertRow) {
			throw new SQLException(MESSAGE_INVALID_OPERATION);
		}
		return (((Row) getCurrentRow()).getInserted());
	}

	/**
	 * Indicates whether the current row has been deleted. A deleted row may leave a
	 * visible "hole" in a rowset. This method can be used to detect such holes if
	 * the rowset can detect deletions. This method will always return
	 * <code>false</code> if this rowset cannot detect deletions.
	 *
	 * @return <code>true</code> if (1)the current row is blank, indicating that the
	 *         row has been deleted, and (2)deletions are detected;
	 *         <code>false</code> otherwise
	 * @throws SQLException if the cursor is on a valid row in this rowset
	 * @see DatabaseMetaData#deletesAreDetected
	 */
	public boolean rowDeleted() throws SQLException {
		// make sure the cursor is on a valid row

		if (isAfterLast() || isBeforeFirst() || onInsertRow) {

			throw new SQLException(MESSAGE_INVALID_CURSOR_POSITION);
		}
		return (((Row) getCurrentRow()).getDeleted());
	}

	/**
	 * Indicates whether the given SQL data type is a numberic type.
	 *
	 * @param type one of the constants from <code>java.sql.Types</code>
	 * @return <code>true</code> if the given type is <code>NUMERIC</code>,'
	 *         <code>DECIMAL</code>, <code>BIT</code>, <code>TINYINT</code>,
	 *         <code>SMALLINT</code>, <code>INTEGER</code>, <code>BIGINT</code>,
	 *         <code>REAL</code>, <code>DOUBLE</code>, or <code>FLOAT</code>;
	 *         <code>false</code> otherwise
	 */
	private boolean isNumeric(int type) {
		switch (type) {
		case java.sql.Types.NUMERIC:
		case java.sql.Types.DECIMAL:
		case java.sql.Types.BIT:
		case java.sql.Types.TINYINT:
		case java.sql.Types.SMALLINT:
		case java.sql.Types.INTEGER:
		case java.sql.Types.BIGINT:
		case java.sql.Types.REAL:
		case java.sql.Types.DOUBLE:
		case java.sql.Types.FLOAT:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Indicates whether the given SQL data type is a string type.
	 *
	 * @param type one of the constants from <code>java.sql.Types</code>
	 * @return <code>true</code> if the given type is <code>CHAR</code>,'
	 *         <code>VARCHAR</code>, or <code>LONGVARCHAR</code>; <code>false</code>
	 *         otherwise
	 */
	private boolean isString(int type) {
		switch (type) {
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
		case java.sql.Types.LONGVARCHAR:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Indicates whether the given SQL data type is a binary type.
	 *
	 * @param type one of the constants from <code>java.sql.Types</code>
	 * @return <code>true</code> if the given type is <code>BINARY</code>,'
	 *         <code>VARBINARY</code>, or <code>LONGVARBINARY</code>;
	 *         <code>false</code> otherwise
	 */
	private boolean isBinary(int type) {
		switch (type) {
		case java.sql.Types.BINARY:
		case java.sql.Types.VARBINARY:
		case java.sql.Types.LONGVARBINARY:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Indicates whether the given SQL data type is a temporal type. This method is
	 * called internally by the conversion methods <code>convertNumeric</code> and
	 * <code>convertTemporal</code>.
	 *
	 * @param type one of the constants from <code>java.sql.Types</code>
	 * @return <code>true</code> if the given type is <code>DATE</code>,
	 *         <code>TIME</code>, or <code>TIMESTAMP</code>; <code>false</code>
	 *         otherwise
	 */
	private boolean isTemporal(int type) {
		switch (type) {
		case java.sql.Types.DATE:
		case java.sql.Types.TIME:
		case java.sql.Types.TIMESTAMP:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Indicates whether the given SQL data type is a boolean type. This method is
	 * called internally by the conversion methods <code>convertNumeric</code> and
	 * <code>convertBoolean</code>.
	 *
	 * @param type one of the constants from <code>java.sql.Types</code>
	 * @return <code>true</code> if the given type is <code>BIT</code>, , or
	 *         <code>BOOLEAN</code>; <code>false</code> otherwise
	 */
	private boolean isBoolean(int type) {
		switch (type) {
		case java.sql.Types.BIT:
		case java.sql.Types.BOOLEAN:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Converts the given <code>Object</code> in the Java programming language to
	 * the standard mapping for the specified SQL target data type. The conversion
	 * must be to a string or numeric type, but there are no restrictions on the
	 * type to be converted. If the source type and target type are the same, the
	 * given object is simply returned.
	 *
	 * @param srcObj  the <code>Object</code> in the Java programming language that
	 *                is to be converted to the target type
	 * @param srcType the data type that is the standard mapping in SQL of the
	 *                object to be converted; must be one of the constants in
	 *                <code>java.sql.Types</code>
	 * @param trgType the SQL data type to which to convert the given object; must
	 *                be one of the following constants in
	 *                <code>java.sql.Types</code>: <code>NUMERIC</code>,
	 *                <code>DECIMAL</code>, <code>BIT</code>, <code>TINYINT</code>,
	 *                <code>SMALLINT</code>, <code>INTEGER</code>,
	 *                <code>BIGINT</code>, <code>REAL</code>, <code>DOUBLE</code>,
	 *                <code>FLOAT</code>, <code>VARCHAR</code>,
	 *                <code>LONGVARCHAR</code>, or <code>CHAR</code>
	 * @return an <code>Object</code> value.that is the standard object mapping for
	 *         the target SQL type
	 * @throws SQLException if the given target type is not one of the string or
	 *                      numeric types in <code>java.sql.Types</code>
	 */
	private Object convertNumeric(Object srcObj, int srcType, int trgType) throws SQLException {

		if (srcType == trgType) {
			return srcObj;
		}

		if (!isNumeric(trgType) && !isString(trgType)) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH + ": " + trgType);
		}

		try {
			switch (trgType) {
			case java.sql.Types.BIT:
				Integer i = Integer.valueOf(srcObj.toString().trim());
				return i.equals(0) ? Boolean.valueOf(false) : Boolean.valueOf(true);
			case java.sql.Types.TINYINT:
				return Byte.valueOf(srcObj.toString().trim());
			case java.sql.Types.SMALLINT:
				return Short.valueOf(srcObj.toString().trim());
			case java.sql.Types.INTEGER:
				return Integer.valueOf(srcObj.toString().trim());
			case java.sql.Types.BIGINT:
				return Long.valueOf(srcObj.toString().trim());
			case java.sql.Types.NUMERIC:
			case java.sql.Types.DECIMAL:
				return new BigDecimal(srcObj.toString().trim());
			case java.sql.Types.REAL:
			case java.sql.Types.FLOAT:
				return Float.valueOf(srcObj.toString().trim());
			case java.sql.Types.DOUBLE:
				return Double.valueOf(srcObj.toString().trim());
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
				return srcObj.toString();
			default:
				throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH + ": " + trgType);
			}
		} catch (NumberFormatException ex) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH + ": " + trgType);
		}
	}

	/**
	 * Converts the given <code>Object</code> in the Java programming language to
	 * the standard object mapping for the specified SQL target data type. The
	 * conversion must be to a string or temporal type, and there are also
	 * restrictions on the type to be converted.
	 * <P>
	 * <TABLE ALIGN="CENTER" BORDER CELLPADDING=10 BORDERCOLOR="#0000FF"
	 * <CAPTION ALIGN="CENTER"><B>Parameters and Return Values</B></CAPTION>
	 * <TR>
	 * <TD><B>Source SQL Type</B>
	 * <TD><B>Target SQL Type</B>
	 * <TD><B>Object Returned</B>
	 * </TR>
	 * <TR>
	 * <TD><code>TIMESTAMP</code>
	 * <TD><code>DATE</code>
	 * <TD><code>java.sql.Date</code>
	 * </TR>
	 * <TR>
	 * <TD><code>TIMESTAMP</code>
	 * <TD><code>TIME</code>
	 * <TD><code>java.sql.Time</code>
	 * </TR>
	 * <TR>
	 * <TD><code>TIME</code>
	 * <TD><code>TIMESTAMP</code>
	 * <TD><code>java.sql.Timestamp</code>
	 * </TR>
	 * <TR>
	 * <TD><code>DATE</code>, <code>TIME</code>, or <code>TIMESTAMP</code>
	 * <TD><code>CHAR</code>, <code>VARCHAR</code>, or <code>LONGVARCHAR</code>
	 * <TD><code>java.lang.String</code>
	 * </TR>
	 * </TABLE>
	 * <P>
	 * If the source type and target type are the same, the given object is simply
	 * returned.
	 *
	 * @param srcObj  the <code>Object</code> in the Java programming language that
	 *                is to be converted to the target type
	 * @param srcType the data type that is the standard mapping in SQL of the
	 *                object to be converted; must be one of the constants in
	 *                <code>java.sql.Types</code>
	 * @param trgType the SQL data type to which to convert the given object; must
	 *                be one of the following constants in
	 *                <code>java.sql.Types</code>: <code>DATE</code>,
	 *                <code>TIME</code>, <code>TIMESTAMP</code>, <code>CHAR</code>,
	 *                <code>VARCHAR</code>, or <code>LONGVARCHAR</code>
	 * @return an <code>Object</code> value.that is the standard object mapping for
	 *         the target SQL type
	 * @throws SQLException if the given target type is not one of the string or
	 *                      temporal types in <code>java.sql.Types</code>
	 */
	private Object convertTemporal(Object srcObj, int srcType, int trgType) throws SQLException {

		if (srcType == trgType) {
			return srcObj;
		}

		if (isNumeric(trgType) || (!isString(trgType) && !isTemporal(trgType))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		try {
			switch (trgType) {
			case java.sql.Types.DATE:
				if (srcType == java.sql.Types.TIMESTAMP) {
					return new java.sql.Date(((java.sql.Timestamp) srcObj).getTime());
				} else {
					throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
				}
			case java.sql.Types.TIMESTAMP:
				if (srcType == java.sql.Types.TIME) {
					return new Timestamp(((java.sql.Time) srcObj).getTime());
				} else {
					return new Timestamp(((java.sql.Date) srcObj).getTime());
				}
			case java.sql.Types.TIME:
				if (srcType == java.sql.Types.TIMESTAMP) {
					return new Time(((java.sql.Timestamp) srcObj).getTime());
				} else {
					throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
				}
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
				return srcObj.toString();
			default:
				throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
			}
		} catch (NumberFormatException ex) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

	}

	/**
	 * Converts the given <code>Object</code> in the Java programming language to
	 * the standard mapping for the specified SQL target data type. The conversion
	 * must be to a string or numeric type, but there are no restrictions on the
	 * type to be converted. If the source type and target type are the same, the
	 * given object is simply returned.
	 *
	 * @param srcObj  the <code>Object</code> in the Java programming language that
	 *                is to be converted to the target type
	 * @param srcType the data type that is the standard mapping in SQL of the
	 *                object to be converted; must be one of the constants in
	 *                <code>java.sql.Types</code>
	 * @param trgType the SQL data type to which to convert the given object; must
	 *                be one of the following constants in
	 *                <code>java.sql.Types</code>: <code>BIT</code>, or
	 *                <code>BOOLEAN</code>
	 * @return an <code>Object</code> value.that is the standard object mapping for
	 *         the target SQL type
	 * @throws SQLException if the given target type is not one of the Boolean types
	 *                      in <code>java.sql.Types</code>
	 */
	private Object convertBoolean(Object srcObj, int srcType, int trgType) throws SQLException {

		if (srcType == trgType) {
			return srcObj;
		}

		if (isNumeric(trgType) || (!isString(trgType) && !isBoolean(trgType))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		try {
			switch (trgType) {
			case java.sql.Types.BIT:
				Integer i = Integer.valueOf(srcObj.toString().trim());
				return i.equals(0) ? Boolean.valueOf(false) : Boolean.valueOf(true);
			case java.sql.Types.BOOLEAN:
				return Boolean.valueOf(srcObj.toString().trim());
			default:
				throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH + trgType);
			}
		} catch (NumberFormatException ex) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH + trgType);
		}
	}

	/**
	 * Sets the designated nullable column in the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with <code>null</code> value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset; however, another method must be called to complete the update
	 * process. If the cursor is on a row in the rowset, the method
	 * {@link #updateRow} must be called to mark the row as updated and to notify
	 * listeners that the row has changed. If the cursor is on the insert row, the
	 * method {@link #insertRow} must be called to insert the new row into this
	 * rowset and to notify listeners that a row has changed.
	 * <P>
	 * In order to propagate updates in this rowset to the underlying data source,
	 * an application must call the method {@link #acceptChanges} after it calls
	 * either <code>updateRow</code> or <code>insertRow</code>.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateNull(int columnIndex) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		BaseRow row = getCurrentRow();
		row.setColumnObject(columnIndex, null);

	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>boolean</code>
	 * value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();
		Object obj = convertBoolean(Boolean.valueOf(x), java.sql.Types.BIT, rowSetMetaData.getColumnType(columnIndex));

		getCurrentRow().setColumnObject(columnIndex, obj);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>byte</code>
	 * value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateByte(int columnIndex, byte x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		Object obj = convertNumeric(Byte.valueOf(x), java.sql.Types.TINYINT, rowSetMetaData.getColumnType(columnIndex));

		getCurrentRow().setColumnObject(columnIndex, obj);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>short</code>
	 * value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateShort(int columnIndex, short x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		Object obj = convertNumeric(Short.valueOf(x), java.sql.Types.SMALLINT,
				rowSetMetaData.getColumnType(columnIndex));

		getCurrentRow().setColumnObject(columnIndex, obj);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>int</code>
	 * value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateInt(int columnIndex, int x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();
		Object obj = convertNumeric(x, java.sql.Types.INTEGER, rowSetMetaData.getColumnType(columnIndex));

		getCurrentRow().setColumnObject(columnIndex, obj);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>long</code>
	 * value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateLong(int columnIndex, long x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		Object obj = convertNumeric(Long.valueOf(x), java.sql.Types.BIGINT, rowSetMetaData.getColumnType(columnIndex));

		getCurrentRow().setColumnObject(columnIndex, obj);

	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>float</code>
	 * value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateFloat(int columnIndex, float x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		Object obj = convertNumeric(Float.valueOf(x), java.sql.Types.REAL, rowSetMetaData.getColumnType(columnIndex));

		getCurrentRow().setColumnObject(columnIndex, obj);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>double</code>
	 * value.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateDouble(int columnIndex, double x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();
		Object obj = convertNumeric(Double.valueOf(x), java.sql.Types.DOUBLE,
				rowSetMetaData.getColumnType(columnIndex));

		getCurrentRow().setColumnObject(columnIndex, obj);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given
	 * <code>java.math.BigDecimal</code> object.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		Object obj = convertNumeric(x, java.sql.Types.NUMERIC, rowSetMetaData.getColumnType(columnIndex));

		getCurrentRow().setColumnObject(columnIndex, obj);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>String</code>
	 * object.
	 * <P>
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to mark the
	 * row as updated. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called to insert the new row into this rowset and
	 * mark it as inserted. Both of these methods must be called before the cursor
	 * moves to another row.
	 * <P>
	 * The method <code>acceptChanges</code> must be called if the updated values
	 * are to be written back to the underlying database.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateString(int columnIndex, String x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		getCurrentRow().setColumnObject(columnIndex, x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>byte</code>
	 * array.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (!isBinary(rowSetMetaData.getColumnType(columnIndex))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		getCurrentRow().setColumnObject(columnIndex, x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>Date</code>
	 * object.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, (3) the type of the designated column is not an SQL
	 *                      <code>DATE</code> or <code>TIMESTAMP</code>, or (4) this
	 *                      rowset is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		Object obj = convertTemporal(x, java.sql.Types.DATE, rowSetMetaData.getColumnType(columnIndex));

		getCurrentRow().setColumnObject(columnIndex, obj);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>Time</code>
	 * object.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, (3) the type of the designated column is not an SQL
	 *                      <code>TIME</code> or <code>TIMESTAMP</code>, or (4) this
	 *                      rowset is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateTime(int columnIndex, java.sql.Time x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		Object obj = convertTemporal(x, java.sql.Types.TIME, rowSetMetaData.getColumnType(columnIndex));

		getCurrentRow().setColumnObject(columnIndex, obj);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given
	 * <code>Timestamp</code> object.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, (3) the type of the designated column is not an SQL
	 *                      <code>DATE</code>, <code>TIME</code>, or
	 *                      <code>TIMESTAMP</code>, or (4) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateTimestamp(int columnIndex, java.sql.Timestamp x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		Object obj = convertTemporal(x, java.sql.Types.TIMESTAMP, rowSetMetaData.getColumnType(columnIndex));

		getCurrentRow().setColumnObject(columnIndex, obj);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given ASCII stream value.
	 * <P>
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @param length      the number of one-byte ASCII characters in the stream
	 * @throws SQLException if this method is invoked
	 */
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		// sanity Check
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (!isString(rowSetMetaData.getColumnType(columnIndex))
				&& !isBinary(rowSetMetaData.getColumnType(columnIndex))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		byte[] buf = new byte[length];
		try {
			int charsRead = 0;
			do {
				charsRead += x.read(buf, charsRead, length - charsRead);
			} while (charsRead != length);
			// Changed the condition check to check for length instead of -1
		} catch (IOException ex) {
			throw new SQLException("read failed for ascii stream");
		}
		String str = new String(buf);

		getCurrentRow().setColumnObject(columnIndex, str);

	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given
	 * <code>InputStream</code> object.
	 * <P>
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value; must be a <code>InputStream</code>
	 *                    containing <code>BINARY</code>, <code>VARBINARY</code>, or
	 *                    <code>LONGVARBINARY</code> data
	 * @param length      the length of the stream in bytes
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, (3) the data in the stream is not binary, or (4)
	 *                      this rowset is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		// sanity Check
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (!isBinary(rowSetMetaData.getColumnType(columnIndex))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		byte[] buf = new byte[length];
		try {
			int bytesRead = 0;
			do {
				bytesRead += x.read(buf, bytesRead, length - bytesRead);
			} while (bytesRead != -1);
		} catch (IOException ex) {
			throw new SQLException("read failed for binary stream");
		}

		getCurrentRow().setColumnObject(columnIndex, buf);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>Reader</code>
	 * object.
	 * <P>
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value; must be a <code>Reader</code>
	 *                    containing <code>BINARY</code>, <code>VARBINARY</code>,
	 *                    <code>LONGVARBINARY</code>, <code>CHAR</code>,
	 *                    <code>VARCHAR</code>, or <code>LONGVARCHAR</code> data
	 * @param length      the length of the stream in characters
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, (3) the data in the stream is not a binary or
	 *                      character type, or (4) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		// sanity Check
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (!isString(rowSetMetaData.getColumnType(columnIndex))
				&& !isBinary(rowSetMetaData.getColumnType(columnIndex))) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		char[] buf = new char[length];
		try {
			int charsRead = 0;
			do {
				charsRead += x.read(buf, charsRead, length - charsRead);
			} while (charsRead != length);
			// Changed the condition checking to check for length instead of -1
		} catch (IOException ex) {
			throw new SQLException("read failed for binary stream");
		}
		String str = new String(buf);

		getCurrentRow().setColumnObject(columnIndex, str);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>Object</code>
	 * value. The <code>scale</code> parameter indicates the number of digits to the
	 * right of the decimal point and is ignored if the new column value is not a
	 * type that will be mapped to an SQL <code>DECIMAL</code> or
	 * <code>NUMERIC</code> value.
	 * <P>
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @param scale       the number of digits to the right of the decimal point
	 *                    (for <code>DECIMAL</code> and <code>NUMERIC</code> types
	 *                    only)
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		int type = rowSetMetaData.getColumnType(columnIndex);
		if (type == Types.DECIMAL || type == Types.NUMERIC) {
			((java.math.BigDecimal) x).setScale(scale);
		}
		getCurrentRow().setColumnObject(columnIndex, x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>Object</code>
	 * value.
	 * <P>
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param x           the new column value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateObject(int columnIndex, Object x) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		getCurrentRow().setColumnObject(columnIndex, x);
	}

	/**
	 * Sets the designated nullable column in the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with <code>null</code> value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateNull(String columnName) throws SQLException {
		updateNull(getColIdxByName(columnName));
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>boolean</code>
	 * value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateBoolean(String columnName, boolean x) throws SQLException {
		updateBoolean(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>byte</code>
	 * value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateByte(String columnName, byte x) throws SQLException {
		updateByte(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>short</code>
	 * value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateShort(String columnName, short x) throws SQLException {
		updateShort(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>int</code>
	 * value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateInt(String columnName, int x) throws SQLException {
		updateInt(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>long</code>
	 * value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateLong(String columnName, long x) throws SQLException {
		updateLong(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>float</code>
	 * value.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateFloat(String columnName, float x) throws SQLException {
		updateFloat(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>double</code>
	 * value.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateDouble(String columnName, double x) throws SQLException {
		updateDouble(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given
	 * <code>java.math.BigDecimal</code> object.
	 * <P>
	 * This method updates a column value in the current row or the insert row of
	 * this rowset, but it does not update the database. If the cursor is on a row
	 * in the rowset, the method {@link #updateRow} must be called to update the
	 * database. If the cursor is on the insert row, the method {@link #insertRow}
	 * must be called, which will insert the new row into both this rowset and the
	 * database. Both of these methods must be called before the cursor moves to
	 * another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
		updateBigDecimal(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>String</code>
	 * object.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateString(String columnName, String x) throws SQLException {
		updateString(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>byte</code>
	 * array.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateBytes(String columnName, byte[] x) throws SQLException {
		updateBytes(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>Date</code>
	 * object.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, (3) the type of
	 *                      the designated column is not an SQL <code>DATE</code> or
	 *                      <code>TIMESTAMP</code>, or (4) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateDate(String columnName, java.sql.Date x) throws SQLException {
		updateDate(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>Time</code>
	 * object.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, (3) the type of
	 *                      the designated column is not an SQL <code>TIME</code> or
	 *                      <code>TIMESTAMP</code>, or (4) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateTime(String columnName, java.sql.Time x) throws SQLException {
		updateTime(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given
	 * <code>Timestamp</code> object.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if the given column index is out of bounds or the cursor
	 *                      is not on one of this rowset's rows or its insert row
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, (3) the type of
	 *                      the designated column is not an SQL <code>DATE</code>,
	 *                      <code>TIME</code>, or <code>TIMESTAMP</code>, or (4)
	 *                      this rowset is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateTimestamp(String columnName, java.sql.Timestamp x) throws SQLException {
		updateTimestamp(getColIdxByName(columnName), x);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given ASCII stream value.
	 * <P>
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @param length     the number of one-byte ASCII characters in the stream
	 */
	public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
		updateAsciiStream(getColIdxByName(columnName), x, length);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given
	 * <code>InputStream</code> object.
	 * <P>
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value; must be a <code>InputStream</code>
	 *                   containing <code>BINARY</code>, <code>VARBINARY</code>, or
	 *                   <code>LONGVARBINARY</code> data
	 * @param length     the length of the stream in bytes
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, (3) the data in
	 *                      the stream is not binary, or (4) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
		updateBinaryStream(getColIdxByName(columnName), x, length);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>Reader</code>
	 * object.
	 * <P>
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param reader     the new column value; must be a <code>Reader</code>
	 *                   containing <code>BINARY</code>, <code>VARBINARY</code>,
	 *                   <code>LONGVARBINARY</code>, <code>CHAR</code>,
	 *                   <code>VARCHAR</code>, or <code>LONGVARCHAR</code> data
	 * @param length     the length of the stream in characters
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, (3) the data in
	 *                      the stream is not a binary or character type, or (4)
	 *                      this rowset is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
		updateCharacterStream(getColIdxByName(columnName), reader, length);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>Object</code>
	 * value. The <code>scale</code> parameter indicates the number of digits to the
	 * right of the decimal point and is ignored if the new column value is not a
	 * type that will be mapped to an SQL <code>DECIMAL</code> or
	 * <code>NUMERIC</code> value.
	 * <P>
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @param scale      the number of digits to the right of the decimal point (for
	 *                   <code>DECIMAL</code> and <code>NUMERIC</code> types only)
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateObject(String columnName, Object x, int scale) throws SQLException {
		updateObject(getColIdxByName(columnName), x, scale);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>Object</code>
	 * value.
	 * <P>
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param x          the new column value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateObject(String columnName, Object x) throws SQLException {
		updateObject(getColIdxByName(columnName), x);
	}

	/**
	 * Inserts the contents of this <code>CachedRowSetImpl</code> object's insert
	 * row into this rowset immediately following the current row. If the current
	 * row is the position after the last row or before the first row, the new row
	 * will be inserted at the end of the rowset. This method also notifies
	 * listeners registered with this rowset that the row has changed.
	 * <P>
	 * The cursor must be on the insert row when this method is called.
	 *
	 * @throws SQLException if (1) the cursor is not on the insert row, (2) one or
	 *                      more of the non-nullable columns in the insert row has
	 *                      not been given a value, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void insertRow() throws SQLException {
		int pos;

		if (!onInsertRow || !insertRow.isCompleteRow(rowSetMetaData)) {
			throw new SQLException("Failed on insert row");
		}
		// Added the setting of parameters that are passed
		// to setXXX methods after an empty CRS Object is
		// created through RowSetMetaData object
		Object[] toInsert = getParams();

		for (int i = 0; i < toInsert.length; i++) {
			insertRow.setColumnObject(i + 1, toInsert[i]);
		}

		Row insRow = new Row(rowSetMetaData.getColumnCount(), insertRow.getOrigRow());
		insRow.setInserted();
		/*
		 * The new row is inserted into the RowSet immediately following the current
		 * row.
		 *
		 * If we are afterlast then the rows are inserted at the end.
		 */
		if (currentRow >= numRows || currentRow < 0) {
			pos = numRows;
		} else {
			pos = currentRow;
		}

		rvh.add(pos, insRow);
		++numRows;
		// notify the listeners that the row changed.
		notifyRowChanged();
	}

	/**
	 * Marks the current row of this <code>CachedRowSetImpl</code> object as updated
	 * and notifies listeners registered with this rowset that the row has changed.
	 * <P>
	 * This method cannot be called when the cursor is on the insert row, and it
	 * should be called before the cursor moves to another row. If it is called
	 * after the cursor moves to another row, this method has no effect, and the
	 * updates made before the cursor moved will be lost.
	 *
	 * @throws SQLException if the cursor is on the insert row or this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateRow() throws SQLException {
		// make sure we aren't on the insert row
		if (onInsertRow) {
			throw new SQLException("pdateRow called while on insert row");
		}

		((Row) getCurrentRow()).setUpdated();

		// notify the listeners that the row changed.
		notifyRowChanged();
	}

	/**
	 * Deletes the current row from this <code>CachedRowSetImpl</code> object and
	 * notifies listeners registered with this rowset that a row has changed. This
	 * method cannot be called when the cursor is on the insert row.
	 * <P>
	 * This method marks the current row as deleted, but it does not delete the row
	 * from the underlying data source. The method <code>acceptChanges</code> must
	 * be called to delete the row in the data source.
	 *
	 * @throws SQLException if (1) this method is called when the cursor is on the
	 *                      insert row, before the first row, or after the last row
	 *                      or (2) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void deleteRow() throws SQLException {
		// make sure the cursor is on a valid row
		checkCursor();

		((Row) getCurrentRow()).setDeleted();
		++numDeleted;

		// notify the listeners that the row changed.
		notifyRowChanged();
	}

	/**
	 * Sets the current row with its original value and marks the row as not
	 * updated, thus undoing any changes made to the row since the last call to the
	 * methods <code>updateRow</code> or <code>deleteRow</code>. This method should
	 * be called only when the cursor is on a row in this rowset.
	 *
	 * @throws SQLException if the cursor is on the insert row, before the first
	 *                      row, or after the last row
	 */
	public void refreshRow() throws SQLException {
		// make sure we are on a row
		checkCursor();

		// don't want this to happen...
		if (onInsertRow) {
			throw new SQLException(MESSAGE_INVALID_CURSOR_POSITION);
		}

		// just undo any changes made to this row.
		((Row) getCurrentRow()).clearUpdated();
	}

	/**
	 * Rolls back any updates made to the current row of this
	 * <code>CachedRowSetImpl</code> object and notifies listeners that a row has
	 * changed. To have an effect, this method must be called after an
	 * <code>updateXXX</code> method has been called and before the method
	 * <code>updateRow</code> has been called. If no updates have been made or the
	 * method <code>updateRow</code> has already been called, this method has no
	 * effect.
	 *
	 * @throws SQLException if the cursor is on the insert row, before the first
	 *                      row, or after the last row
	 */
	public void cancelRowUpdates() throws SQLException {
		// make sure we are on a row
		checkCursor();

		// don't want this to happen...
		if (onInsertRow) {
			throw new SQLException(MESSAGE_INVALID_CURSOR_POSITION);
		}

		Row row = (Row) getCurrentRow();
		if (row.getUpdated()) {
			row.clearUpdated();
			notifyRowChanged();
		}
	}

	/**
	 * Moves the cursor for this <code>CachedRowSetImpl</code> object to the insert
	 * row. The current row in the rowset is remembered while the cursor is on the
	 * insert row.
	 * <P>
	 * The insert row is a special row associated with an updatable rowset. It is
	 * essentially a buffer where a new row may be constructed by calling the
	 * appropriate <code>updateXXX</code> methods to assign a value to each column
	 * in the row. A complete row must be constructed; that is, every column that is
	 * not nullable must be assigned a value. In order for the new row to become
	 * part of this rowset, the method <code>insertRow</code> must be called before
	 * the cursor is moved back to the rowset.
	 * <P>
	 * Only certain methods may be invoked while the cursor is on the insert row;
	 * many methods throw an exception if they are called while the cursor is there.
	 * In addition to the <code>updateXXX</code> and <code>insertRow</code> methods,
	 * only the <code>getXXX</code> methods may be called when the cursor is on the
	 * insert row. A <code>getXXX</code> method should be called on a column only
	 * after an <code>updateXXX</code> method has been called on that column;
	 * otherwise, the value returned is undetermined.
	 *
	 * @throws SQLException if this <code>CachedRowSetImpl</code> object is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void moveToInsertRow() throws SQLException {
		if (getConcurrency() == ResultSet.CONCUR_READ_ONLY) {
			throw new SQLException("moveToInsertRow: CONCUR_READ_ONLY");
		}
		if (insertRow == null) {
			if (rowSetMetaData == null)
				throw new SQLException("moveToInsertRow: no meta data");
			int numCols = rowSetMetaData.getColumnCount();
			if (numCols > 0) {
				insertRow = new InsertRow(numCols);
			} else {
				throw new SQLException("moveToInsertRow: invalid number of columns");
			}
		}
		onInsertRow = true;
		// %%% setCurrentRow called in BaseRow

		currentRow = cursorPos;
		cursorPos = -1;

		insertRow.initInsertRow();
	}

	/**
	 * Moves the cursor for this <code>CachedRowSetImpl</code> object to the current
	 * row. The current row is the row the cursor was on when the method
	 * <code>moveToInsertRow</code> was called.
	 * <P>
	 * Calling this method has no effect unless it is called while the cursor is on
	 * the insert row.
	 *
	 * @throws SQLException if an error occurs
	 */
	public void moveToCurrentRow() throws SQLException {
		if (onInsertRow) {
			cursorPos = currentRow;
			onInsertRow = false;
		}
	}

	/**
	 * Returns <code>null</code>.
	 *
	 * @return <code>null</code>
	 * @throws SQLException if an error occurs
	 */
	public Statement getStatement() throws SQLException {
		return null;
	}

	/**
	 * Retrieves the value of the designated column in this
	 * <code>CachedRowSetImpl</code> object as an <code>Object</code> in the Java
	 * programming language, using the given <code>java.util.Map</code> object to
	 * custom map the value if appropriate.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param map         a <code>java.util.Map</code> object showing the mapping
	 *                    from SQL type names to classes in the Java programming
	 *                    language
	 * @return an <code>Object</code> representing the SQL value
	 * @throws SQLException if the given column index is out of bounds or the cursor
	 *                      is not on one of this rowset's rows or its insert row
	 */
	public Object getObject(int columnIndex, java.util.Map<String, Class<?>> map) throws SQLException {
		return getValue(columnIndex, null, value -> {
			if (value instanceof Struct) {
				Struct s = (Struct) value;

				// look up the class in the map
				Class<?> c = map.get(s.getSQLTypeName());
				if (c != null) {
					// create new instance of the class
					SQLData obj = null;
					try {
						@SuppressWarnings("deprecation")
						Object tmp = c.newInstance();
						obj = (SQLData) tmp;
					} catch (Exception ex) {
						throw new SQLException("Unable to Instantiate: ", ex);
					}
					// get the attributes from the struct
					Object[] attribs = s.getAttributes(map);
					// create the SQLInput "stream"
					SQLInputImpl sqlInput = new SQLInputImpl(attribs, map);
					// read the values...
					obj.readSQL(sqlInput, s.getSQLTypeName());
					return obj;
				}
			}
			return value;
		});
	}

	/**
	 * Retrieves the value of the designated column in this
	 * <code>CachedRowSetImpl</code> object as a <code>Ref</code> object in the Java
	 * programming language.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @return a <code>Ref</code> object representing an SQL<code> REF</code> value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>REF</code> value
	 * @see #getRef(String)
	 */
	public Ref getRef(int columnIndex) throws SQLException {
		Ref value;

		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (rowSetMetaData.getColumnType(columnIndex) != java.sql.Types.REF) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		setLastValueNull(false);
		value = (Ref) (getCurrentRow().getColumnObject(columnIndex));

		// check for SQL NULL
		if (value == null) {
			setLastValueNull(true);
			return null;
		}

		return value;
	}

	/**
	 * Retrieves the value of the designated column in this
	 * <code>CachedRowSetImpl</code> object as a <code>Blob</code> object in the
	 * Java programming language.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @return a <code>Blob</code> object representing an SQL <code>BLOB</code>
	 *         value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>BLOB</code> value
	 * @see #getBlob(String)
	 */
	public Blob getBlob(int columnIndex) throws SQLException {
		Blob value;

		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (rowSetMetaData.getColumnType(columnIndex) != java.sql.Types.BLOB) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		setLastValueNull(false);
		value = (Blob) (getCurrentRow().getColumnObject(columnIndex));

		// check for SQL NULL
		if (value == null) {
			setLastValueNull(true);
			return null;
		}

		return value;
	}

	/**
	 * Retrieves the value of the designated column in this
	 * <code>CachedRowSetImpl</code> object as a <code>Clob</code> object in the
	 * Java programming language.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @return a <code>Clob</code> object representing an SQL <code>CLOB</code>
	 *         value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>CLOB</code> value
	 * @see #getClob(String)
	 */
	public Clob getClob(int columnIndex) throws SQLException {
		Clob value;

		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (rowSetMetaData.getColumnType(columnIndex) != java.sql.Types.CLOB) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		setLastValueNull(false);
		value = (Clob) (getCurrentRow().getColumnObject(columnIndex));

		// check for SQL NULL
		if (value == null) {
			setLastValueNull(true);
			return null;
		}

		return value;
	}

	/**
	 * Retrieves the value of the designated column in this
	 * <code>CachedRowSetImpl</code> object as an <code>Array</code> object in the
	 * Java programming language.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @return an <code>Array</code> object representing an SQL <code>ARRAY</code>
	 *         value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>ARRAY</code> value
	 * @see #getArray(String)
	 */
	public Array getArray(int columnIndex) throws SQLException {
		java.sql.Array value;

		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (rowSetMetaData.getColumnType(columnIndex) != java.sql.Types.ARRAY) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		setLastValueNull(false);
		value = (java.sql.Array) (getCurrentRow().getColumnObject(columnIndex));

		// check for SQL NULL
		if (value == null) {
			setLastValueNull(true);
			return null;
		}

		return value;
	}

	/**
	 * Retrieves the value of the designated column in this
	 * <code>CachedRowSetImpl</code> object as an <code>Object</code> in the Java
	 * programming language, using the given <code>java.util.Map</code> object to
	 * custom map the value if appropriate.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param map        a <code>java.util.Map</code> object showing the mapping
	 *                   from SQL type names to classes in the Java programming
	 *                   language
	 * @return an <code>Object</code> representing the SQL value
	 * @throws SQLException if the given column name is not the name of a column in
	 *                      this rowset or the cursor is not on one of this rowset's
	 *                      rows or its insert row
	 */
	public Object getObject(String columnName, java.util.Map<String, Class<?>> map) throws SQLException {
		return getObject(getColIdxByName(columnName), map);
	}

	/**
	 * Retrieves the value of the designated column in this
	 * <code>CachedRowSetImpl</code> object as a <code>Ref</code> object in the Java
	 * programming language.
	 *
	 * @param colName a <code>String</code> object that must match the SQL name of a
	 *                column in this rowset, ignoring case
	 * @return a <code>Ref</code> object representing an SQL<code> REF</code> value
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the column value
	 *                      is not an SQL <code>REF</code> value
	 * @see #getRef(int)
	 */
	public Ref getRef(String colName) throws SQLException {
		return getRef(getColIdxByName(colName));
	}

	/**
	 * Retrieves the value of the designated column in this
	 * <code>CachedRowSetImpl</code> object as a <code>Blob</code> object in the
	 * Java programming language.
	 *
	 * @param colName a <code>String</code> object that must match the SQL name of a
	 *                column in this rowset, ignoring case
	 * @return a <code>Blob</code> object representing an SQL <code>BLOB</code>
	 *         value
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>BLOB</code> value
	 * @see #getBlob(int)
	 */
	public Blob getBlob(String colName) throws SQLException {
		return getBlob(getColIdxByName(colName));
	}

	/**
	 * Retrieves the value of the designated column in this
	 * <code>CachedRowSetImpl</code> object as a <code>Clob</code> object in the
	 * Java programming language.
	 *
	 * @param colName a <code>String</code> object that must match the SQL name of a
	 *                column in this rowset, ignoring case
	 * @return a <code>Clob</code> object representing an SQL <code>CLOB</code>
	 *         value
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>CLOB</code> value
	 * @see #getClob(int)
	 */
	public Clob getClob(String colName) throws SQLException {
		return getClob(getColIdxByName(colName));
	}

	/**
	 * Retrieves the value of the designated column in this
	 * <code>CachedRowSetImpl</code> object as an <code>Array</code> object in the
	 * Java programming langugage.
	 *
	 * @param colName a <code>String</code> object that must match the SQL name of a
	 *                column in this rowset, ignoring case
	 * @return an <code>Array</code> object representing an SQL <code>ARRAY</code>
	 *         value
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>ARRAY</code> value
	 * @see #getArray(int)
	 */
	public Array getArray(String colName) throws SQLException {
		return getArray(getColIdxByName(colName));
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>java.sql.Date</code> object,
	 * using the given <code>Calendar</code> object to construct an appropriate
	 * millisecond value for the date.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @param cal         the <code>java.util.Calendar</code> object to use in
	 *                    constructing the date
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>DATE</code> or
	 *                      <code>TIMESTAMP</code> value
	 */
	public java.sql.Date getDate(int columnIndex, Calendar cal) throws SQLException {

		return getValue(columnIndex, null, value -> {
			value = convertTemporal(value, rowSetMetaData.getColumnType(columnIndex), java.sql.Types.DATE);

			// create a default calendar
			Calendar defaultCal = Calendar.getInstance();
			// set this Calendar to the time we have
			defaultCal.setTime((java.util.Date) value);

			/*
			 * Now we can pull the pieces of the date out of the default calendar and put
			 * them into the user provided calendar
			 */
			cal.set(Calendar.YEAR, defaultCal.get(Calendar.YEAR));
			cal.set(Calendar.MONTH, defaultCal.get(Calendar.MONTH));
			cal.set(Calendar.DAY_OF_MONTH, defaultCal.get(Calendar.DAY_OF_MONTH));

			/*
			 * This looks a little odd but it is correct - Calendar.getTime() returns a
			 * Date...
			 */
			return new java.sql.Date(cal.getTime().getTime());
		});

	}

	interface ValueConverter<T> {

		T convert(Object value) throws SQLException;

	}

	private <T> T getValue(int columnIndex, T defaultValue, ValueConverter<T> converter) throws SQLException {
		Object value;

		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		setLastValueNull(false);
		value = getCurrentRow().getColumnObject(columnIndex);

		// check for SQL NULL
		if (value == null) {
			setLastValueNull(true);
			return defaultValue;
		}
		return converter.convert(value);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>java.sql.Date</code> object,
	 * using the given <code>Calendar</code> object to construct an appropriate
	 * millisecond value for the date.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param cal        the <code>java.util.Calendar</code> object to use in
	 *                   constructing the date
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>DATE</code> or
	 *                      <code>TIMESTAMP</code> value
	 */
	public java.sql.Date getDate(String columnName, Calendar cal) throws SQLException {
		return getDate(getColIdxByName(columnName), cal);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>java.sql.Time</code> object,
	 * using the given <code>Calendar</code> object to construct an appropriate
	 * millisecond value for the date.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @param cal         the <code>java.util.Calendar</code> object to use in
	 *                    constructing the date
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>TIME</code> or
	 *                      <code>TIMESTAMP</code> value
	 */
	public java.sql.Time getTime(int columnIndex, Calendar cal) throws SQLException {
		return getValue(columnIndex, null, value -> {

			value = convertTemporal(value, rowSetMetaData.getColumnType(columnIndex), java.sql.Types.TIME);

			// create a default calendar
			Calendar defaultCal = Calendar.getInstance();
			// set the time in the default calendar
			defaultCal.setTime((java.util.Date) value);

			/*
			 * Now we can pull the pieces of the date out of the default calendar and put
			 * them into the user provided calendar
			 */
			cal.set(Calendar.HOUR_OF_DAY, defaultCal.get(Calendar.HOUR_OF_DAY));
			cal.set(Calendar.MINUTE, defaultCal.get(Calendar.MINUTE));
			cal.set(Calendar.SECOND, defaultCal.get(Calendar.SECOND));

			return new java.sql.Time(cal.getTime().getTime());
		});
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>java.sql.Time</code> object,
	 * using the given <code>Calendar</code> object to construct an appropriate
	 * millisecond value for the date.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param cal        the <code>java.util.Calendar</code> object to use in
	 *                   constructing the date
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>TIME</code> or
	 *                      <code>TIMESTAMP</code> value
	 */
	public java.sql.Time getTime(String columnName, Calendar cal) throws SQLException {
		return getTime(getColIdxByName(columnName), cal);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>java.sql.Timestamp</code>
	 * object, using the given <code>Calendar</code> object to construct an
	 * appropriate millisecond value for the date.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    the rowset
	 * @param cal         the <code>java.util.Calendar</code> object to use in
	 *                    constructing the date
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>TIME</code> or
	 *                      <code>TIMESTAMP</code> value
	 */
	public java.sql.Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		return getValue(columnIndex, null, value -> {

			value = convertTemporal(value, rowSetMetaData.getColumnType(columnIndex), java.sql.Types.TIMESTAMP);

			// create a default calendar
			Calendar defaultCal = Calendar.getInstance();
			// set the time in the default calendar
			defaultCal.setTime((java.util.Date) value);

			/*
			 * Now we can pull the pieces of the date out of the default calendar and put
			 * them into the user provided calendar
			 */
			cal.set(Calendar.YEAR, defaultCal.get(Calendar.YEAR));
			cal.set(Calendar.MONTH, defaultCal.get(Calendar.MONTH));
			cal.set(Calendar.DAY_OF_MONTH, defaultCal.get(Calendar.DAY_OF_MONTH));
			cal.set(Calendar.HOUR_OF_DAY, defaultCal.get(Calendar.HOUR_OF_DAY));
			cal.set(Calendar.MINUTE, defaultCal.get(Calendar.MINUTE));
			cal.set(Calendar.SECOND, defaultCal.get(Calendar.SECOND));

			return new java.sql.Timestamp(cal.getTime().getTime());
		});
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>CachedRowSetImpl</code> object as a <code>java.sql.Timestamp</code>
	 * object, using the given <code>Calendar</code> object to construct an
	 * appropriate millisecond value for the date.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param cal        the <code>java.util.Calendar</code> object to use in
	 *                   constructing the date
	 * @return the column value; if the value is SQL <code>NULL</code>, the result
	 *         is <code>null</code>
	 * @throws SQLException if (1) the given column name is not the name of a column
	 *                      in this rowset, (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>DATE</code>,
	 *                      <code>TIME</code>, or <code>TIMESTAMP</code> value
	 */
	public java.sql.Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
		return getTimestamp(getColIdxByName(columnName), cal);
	}

	/*
	 * RowSetInternal Interface
	 */

	/**
	 * Retrieves the <code>Connection</code> object passed to this
	 * <code>CachedRowSetImpl</code> object. This connection may be used to populate
	 * this rowset with data or to write data back to its underlying data source.
	 *
	 * @return the <code>Connection</code> object passed to this rowset; may be
	 *         <code>null</code> if there is no connection
	 * @throws SQLException if an error occurs
	 */
	public Connection getConnection() throws SQLException {
		return conn;
	}

	/**
	 * Sets the metadata for this <code>CachedRowSetImpl</code> object with the
	 * given <code>RowSetMetaData</code> object.
	 *
	 * @param md a <code>RowSetMetaData</code> object instance containing metadata
	 *           about the columsn in the rowset
	 * @throws SQLException if invalid meta data is supplied to the rowset
	 */
	public void setMetaData(RowSetMetaData md) throws SQLException {
		rowSetMetaData = (RowSetMetaDataImpl) md;
	}

	/**
	 * Returns a result set containing the original value of the rowset. The
	 * original value is the state of the <code>CachedRowSetImpl</code> after the
	 * last population or synchronization (whichever occurred most recently) with
	 * the data source.
	 * <p>
	 * The cursor is positioned before the first row in the result set. Only rows
	 * contained in the result set returned by <code>getOriginal()</code> are said
	 * to have an original value.
	 *
	 * @return the original result set of the rowset
	 * @throws SQLException if an error occurs produce the <code>ResultSet</code>
	 *                      object
	 */
	public ResultSet getOriginal() throws SQLException {
		CachedRowSetImpl crs = new CachedRowSetImpl();
		crs.rowSetMetaData = rowSetMetaData;
		crs.numRows = numRows;
		crs.cursorPos = 0;

		// make sure we don't get someone playing with these
		// %%% is this now necessary ???
		int colCount = rowSetMetaData.getColumnCount();
		Row orig;

		for (Iterator<?> i = rvh.iterator(); i.hasNext();) {
			orig = new Row(colCount, ((Row) i.next()).getOrigRow());
			crs.rvh.add(orig);
		}
		return crs;
	}

	/**
	 * Returns a result set containing the original value of the current row only.
	 * The original value is the state of the <code>CachedRowSetImpl</code> after
	 * the last population or synchronization (whichever occurred most recently)
	 * with the data source.
	 *
	 * @return the original result set of the row
	 * @throws SQLException if there is no current row
	 * @see #setOriginalRow
	 */
	public ResultSet getOriginalRow() throws SQLException {
		CachedRowSetImpl crs = new CachedRowSetImpl();
		crs.rowSetMetaData = rowSetMetaData;
		crs.numRows = 1;
		crs.cursorPos = 0;
		crs.setTypeMap(this.getTypeMap());

		// make sure we don't get someone playing with these
		// %%% is this now necessary ???

		Row orig = new Row(rowSetMetaData.getColumnCount(), getCurrentRow().getOrigRow());

		crs.rvh.add(orig);

		return crs;

	}

	/**
	 * Marks the current row in this rowset as being an original row.
	 *
	 * @throws SQLException if there is no current row
	 * @see #getOriginalRow
	 */
	public void setOriginalRow() throws SQLException {
		if (onInsertRow) {
			throw new SQLException(MESSAGE_INVALID_OPERATION);
		}

		Row row = (Row) getCurrentRow();
		makeRowOriginal(row);

		// this can happen if deleted rows are being shown
		if (row.getDeleted()) {
			removeCurrentRow();
		}
	}

	/**
	 * Makes the given row of this rowset the original row by clearing any settings
	 * that mark the row as having been inserted, deleted, or updated. This method
	 * is called internally by the methods <code>setOriginalRow</code> and
	 * <code>setOriginal</code>.
	 *
	 * @param row the row to be made the original row
	 */
	private void makeRowOriginal(Row row) {
		if (row.getInserted()) {
			row.clearInserted();
		}

		if (row.getUpdated()) {
			row.moveCurrentToOrig();
		}
	}

	/**
	 * Marks all rows in this rowset as being original rows. Any updates made to the
	 * rows become the original values for the rowset. Calls to the method
	 * <code>setOriginal</code> connot be reversed.
	 *
	 * @throws SQLException if an error occurs
	 */
	public void setOriginal() throws SQLException {
		for (Iterator<?> i = rvh.iterator(); i.hasNext();) {
			Row row = (Row) i.next();
			makeRowOriginal(row);
			// remove deleted rows from the collection.
			if (row.getDeleted()) {
				i.remove();
				--numRows;
			}
		}
		numDeleted = 0;

		// notify any listeners that the rowset has changed
		notifyRowSetChanged();
	}

	/**
	 * Returns an identifier for the object (table) that was used to create this
	 * rowset.
	 *
	 * @return a <code>String</code> object that identifies the table from which
	 *         this <code>CachedRowSetImpl</code> object was derived
	 * @throws SQLException if an error occurs
	 */
	public String getTableName() throws SQLException {
		return tableName;
	}

	/**
	 * Sets the identifier for the table from which this rowset was derived to the
	 * given table name.
	 *
	 * @param tabName a <code>String</code> object that identifies the table from
	 *                which this <code>CachedRowSetImpl</code> object was derived
	 * @throws SQLException if an error occurs
	 */
	public void setTableName(String tabName) throws SQLException {
		if (tabName == null)
			throw new SQLException("Table name cannot be null");
		else
			tableName = tabName;
	}

	/**
	 * Returns the columns that make a key to uniquely identify a row in this
	 * <code>CachedRowSetImpl</code> object.
	 *
	 * @return an array of column numbers that constitutes a primary key for this
	 *         rowset. This array should be empty if no column is representitive of
	 *         a primary key
	 * @throws SQLException if the rowset is empty or no columns are designated as
	 *                      primary keys
	 * @see #setKeyColumns
	 */
	public int[] getKeyColumns() throws SQLException {
		int[] keyColumns = this.keyCols;
		return (keyColumns == null) ? null : Arrays.copyOf(keyColumns, keyColumns.length);
	}

	/**
	 * Sets this <code>CachedRowSetImpl</code> object's <code>keyCols</code> field
	 * with the given array of column numbers, which forms a key for uniquely
	 * identifying a row in this rowset.
	 *
	 * @param keys an array of <code>int</code> indicating the columns that form a
	 *             primary key for this <code>CachedRowSetImpl</code> object; every
	 *             element in the array must be greater than <code>0</code> and less
	 *             than or equal to the number of columns in this rowset
	 * @throws SQLException if any of the numbers in the given array is not valid
	 *                      for this rowset
	 * @see #getKeyColumns
	 */
	public void setKeyColumns(int[] keys) throws SQLException {
		int numCols = 0;
		if (rowSetMetaData != null) {
			numCols = rowSetMetaData.getColumnCount();
			if (keys.length > numCols)
				throw new SQLException("Invalid key columns");
		}
		keyCols = new int[keys.length];
		for (int i = 0; i < keys.length; i++) {
			if (rowSetMetaData != null && (keys[i] <= 0 || keys[i] > numCols)) {
				throw new SQLException("Invalid column index " + keys[i]);
			}
			keyCols[i] = keys[i];
		}
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>Ref</code>
	 * value.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param ref         the new column <code>java.sql.Ref</code> value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateRef(int columnIndex, java.sql.Ref ref) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		// SerialClob will help in getting the byte array and storing it.
		// We need to be checking DatabaseMetaData.locatorsUpdatorCopy()
		// or through RowSetMetaData.locatorsUpdatorCopy()
		getCurrentRow().setColumnObject(columnIndex, new SerialRef(ref));
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>double</code>
	 * value.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param ref        the new column <code>java.sql.Ref</code> value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateRef(String columnName, java.sql.Ref ref) throws SQLException {
		updateRef(getColIdxByName(columnName), ref);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>double</code>
	 * value.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param c           the new column <code>Clob</code> value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateClob(int columnIndex, Clob c) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		// SerialClob will help in getting the byte array and storing it.
		// We need to be checking DatabaseMetaData.locatorsUpdatorCopy()
		// or through RowSetMetaData.locatorsUpdatorCopy()

		if (dbmslocatorsUpdateCopy) {
			getCurrentRow().setColumnObject(columnIndex, new SerialClob(c));
		} else {
			throw new SQLException(MESSAGE_OPERATION_NOT_SUPPORTED);
		}
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given <code>double</code>
	 * value.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param c          the new column <code>Clob</code> value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateClob(String columnName, Clob c) throws SQLException {
		updateClob(getColIdxByName(columnName), c);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given
	 * <code>java.sql.Blob</code> value.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param b           the new column <code>Blob</code> value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateBlob(int columnIndex, Blob b) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		// SerialBlob will help in getting the byte array and storing it.
		// We need to be checking DatabaseMetaData.locatorsUpdatorCopy()
		// or through RowSetMetaData.locatorsUpdatorCopy()

		if (dbmslocatorsUpdateCopy) {
			getCurrentRow().setColumnObject(columnIndex, new SerialBlob(b));
		} else {
			throw new SQLException(MESSAGE_OPERATION_NOT_SUPPORTED);
		}
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given
	 * <code>java.sql.Blob </code> value.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param b          the new column <code>Blob</code> value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateBlob(String columnName, Blob b) throws SQLException {
		updateBlob(getColIdxByName(columnName), b);
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given
	 * <code>java.sql.Array</code> values.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnIndex the first column is <code>1</code>, the second is
	 *                    <code>2</code>, and so on; must be <code>1</code> or
	 *                    larger and equal to or less than the number of columns in
	 *                    this rowset
	 * @param a           the new column <code>Array</code> value
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) this rowset is
	 *                      <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateArray(int columnIndex, Array a) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		// SerialArray will help in getting the byte array and storing it.
		// We need to be checking DatabaseMetaData.locatorsUpdatorCopy()
		// or through RowSetMetaData.locatorsUpdatorCopy()
		getCurrentRow().setColumnObject(columnIndex, new SerialArray(a));
	}

	/**
	 * Sets the designated column in either the current row or the insert row of
	 * this <code>CachedRowSetImpl</code> object with the given
	 * <code>java.sql.Array</code> value.
	 *
	 * This method updates a column value in either the current row or the insert
	 * row of this rowset, but it does not update the database. If the cursor is on
	 * a row in the rowset, the method {@link #updateRow} must be called to update
	 * the database. If the cursor is on the insert row, the method
	 * {@link #insertRow} must be called, which will insert the new row into both
	 * this rowset and the database. Both of these methods must be called before the
	 * cursor moves to another row.
	 *
	 * @param columnName a <code>String</code> object that must match the SQL name
	 *                   of a column in this rowset, ignoring case
	 * @param a          the new column <code>Array</code> value
	 * @throws SQLException if (1) the given column name does not match the name of
	 *                      a column in this rowset, (2) the cursor is not on one of
	 *                      this rowset's rows or its insert row, or (3) this rowset
	 *                      is <code>ResultSet.CONCUR_READ_ONLY</code>
	 */
	public void updateArray(String columnName, Array a) throws SQLException {
		updateArray(getColIdxByName(columnName), a);
	}

	/**
	 * Retrieves the value of the designated column in this
	 * <code>CachedRowSetImpl</code> object as a <code>java.net.URL</code> object in
	 * the Java programming language.
	 *
	 * @return a java.net.URL object containing the resource reference described by
	 *         the URL
	 * @throws SQLException if (1) the given column index is out of bounds, (2) the
	 *                      cursor is not on one of this rowset's rows or its insert
	 *                      row, or (3) the designated column does not store an SQL
	 *                      <code>DATALINK</code> value.
	 * @see #getURL(String)
	 */
	public java.net.URL getURL(int columnIndex) throws SQLException {
		// sanity check.
		checkIndex(columnIndex);
		// make sure the cursor is on a valid row
		checkCursor();

		if (rowSetMetaData.getColumnType(columnIndex) != java.sql.Types.DATALINK) {
			throw new SQLException(MESSAGE_DATA_TYPE_MISMATCH);
		}

		setLastValueNull(false);
		java.net.URL value = (java.net.URL) (getCurrentRow().getColumnObject(columnIndex));

		// check for SQL NULL
		if (value == null) {
			setLastValueNull(true);
			return null;
		}

		return value;
	}

	/**
	 * Retrieves the value of the designated column in this
	 * <code>CachedRowSetImpl</code> object as a <code>java.net.URL</code> object in
	 * the Java programming language.
	 *
	 * @return a java.net.URL object containing the resource reference described by
	 *         the URL
	 * @throws SQLException if (1) the given column name not the name of a column in
	 *                      this rowset, or (2) the cursor is not on one of this
	 *                      rowset's rows or its insert row, or (3) the designated
	 *                      column does not store an SQL <code>DATALINK</code>
	 *                      value.
	 * @see #getURL(int)
	 */
	public java.net.URL getURL(String columnName) throws SQLException {
		return getURL(getColIdxByName(columnName));

	}

	/**
	 * The first warning reported by calls on this <code>CachedRowSetImpl</code>
	 * object is returned. Subsequent <code>CachedRowSetImpl</code> warnings will be
	 * chained to this <code>SQLWarning</code>. All <code>RowSetWarnings</code>
	 * warnings are generated in the disconnected environment and remain a seperate
	 * warning chain to that provided by the <code>getWarnings</code> method.
	 *
	 * <P>
	 * The warning chain is automatically cleared each time a new row is read.
	 *
	 * <P>
	 * <B>Note:</B> This warning chain only covers warnings caused by
	 * <code>CachedRowSet</code> (and their child interface) methods. All
	 * <code>SQLWarnings</code> can be obtained using the <code>getWarnings</code>
	 * method which tracks warnings generated by the underlying JDBC driver.
	 * 
	 * @return the first SQLWarning or null
	 *
	 */
	public RowSetWarning getRowSetWarnings() {
		try {
			notifyCursorMoved();
		} catch (SQLException e) {
			// mask exception
		}
		return rowsetWarning;
	}

	/**
	 * The function tries to isolate the tablename when only setCommand is set and
	 * not setTablename is called provided there is only one table name in the query
	 * else just leaves the setting of table name as such. If setTablename is set
	 * later it will over ride this table name value so retrieved.
	 *
	 * @return the tablename if only one table in query else return ""
	 */
	private String buildTableName(String command) {

		// If we have a query from one table,
		// we set the table name implicitly
		// else user has to explicitly set the table name.

		int indexFrom;
		int indexComma;
		String strTablename = "";
		command = command.trim();

		// Query can be a select, insert or update

		if (command.toLowerCase().startsWith("select")) {
			// look for "from" keyword, after that look for a
			// comma after from. If comma is there don't set
			// table name else isolate table name.

			indexFrom = command.toLowerCase().indexOf("from");
			indexComma = command.indexOf(',', indexFrom);

			if (indexComma == -1) {
				// implies only one table
				strTablename = (command.substring(indexFrom + "from".length(), command.length())).trim();

				String tabName = strTablename;

				int idxWhere = tabName.toLowerCase().indexOf("where");

				/**
				 * Adding the addtional check for conditions following the table name. If a
				 * condition is found truncate it.
				 **/

				if (idxWhere != -1) {
					tabName = tabName.substring(0, idxWhere).trim();
				}

				strTablename = tabName;

			}
		}
		return strTablename;
	}

	/**
	 * Commits all changes performed by the <code>acceptChanges()</code> methods
	 *
	 * @see java.sql.Connection#commit
	 */
	public void commit() throws SQLException {
		conn.commit();
	}

	/**
	 * Rolls back all changes performed by the <code>acceptChanges()</code> methods
	 *
	 * @see java.sql.Connection#rollback
	 */
	public void rollback() throws SQLException {
		conn.rollback();
	}

	/**
	 * Rolls back all changes performed by the <code>acceptChanges()</code> to the
	 * last <code>Savepoint</code> transaction marker.
	 *
	 * @see java.sql.Connection#rollback(Savepoint)
	 */
	public void rollback(Savepoint s) throws SQLException {
		conn.rollback(s);
	}

	/**
	 * Unsets the designated parameter to the given int array. This was set using
	 * <code>setMatchColumn</code> as the column which will form the basis of the
	 * join.
	 * <P>
	 * The parameter value unset by this method should be same as was set.
	 *
	 * @param columnIdxes the index into this rowset object's internal
	 *                    representation of parameter values
	 * @throws SQLException if an error occurs or the parameter index is out of
	 *                      bounds or if the columnIdx is not the same as set using
	 *                      <code>setMatchColumn(int [])</code>
	 */
	public void unsetMatchColumn(int[] columnIdxes) throws SQLException {

		int intValue;
		for (int j = 0; j < columnIdxes.length; j++) {
			intValue = (Integer.parseInt(iMatchColumns.get(j).toString()));
			if (columnIdxes[j] != intValue) {
				throw new SQLException(MESSAGE_MATCH_COLUMNS);
			}
		}

		for (int i = 0; i < columnIdxes.length; i++) {
			iMatchColumns.set(i, -1);
		}
	}

	/**
	 * Unsets the designated parameter to the given String array. This was set using
	 * <code>setMatchColumn</code> as the column which will form the basis of the
	 * join.
	 * <P>
	 * The parameter value unset by this method should be same as was set.
	 *
	 * @param columnIdxes the index into this rowset object's internal
	 *                    representation of parameter values
	 * @throws SQLException if an error occurs or the parameter index is out of
	 *                      bounds or if the columnName is not the same as set using
	 *                      <code>setMatchColumn(String [])</code>
	 */
	public void unsetMatchColumn(String[] columnIdxes) throws SQLException {

		for (int j = 0; j < columnIdxes.length; j++) {
			if (!columnIdxes[j].equals(strMatchColumns.get(j))) {
				throw new SQLException(MESSAGE_MATCH_COLUMNS);
			}
		}

		for (int i = 0; i < columnIdxes.length; i++) {
			strMatchColumns.set(i, null);
		}
	}

	/**
	 * Retrieves the column name as <code>String</code> array that was set using
	 * <code>setMatchColumn(String [])</code> for this rowset.
	 *
	 * @return a <code>String</code> array object that contains the column names for
	 *         the rowset which has this the match columns
	 *
	 * @throws SQLException if an error occurs or column name is not set
	 */
	public String[] getMatchColumnNames() throws SQLException {
		if (strMatchColumns == null || strMatchColumns.isEmpty() || strMatchColumns.get(0) == null) {
			throw new SQLException("Set Match columns before getting them");
		}
		return strMatchColumns.toArray(new String[0]);
	}

	/**
	 * Retrieves the column id as <code>int</code> array that was set using
	 * <code>setMatchColumn(int [])</code> for this rowset.
	 *
	 * @return a <code>int</code> array object that contains the column ids for the
	 *         rowset which has this as the match columns.
	 *
	 * @throws SQLException if an error occurs or column index is not set
	 */
	public int[] getMatchColumnIndexes() throws SQLException {

		int[] intTemp = new int[iMatchColumns.size()];
		int intValue;

		intValue = iMatchColumns.get(0);

		if (intValue == -1) {
			throw new SQLException("Set Match columns before getting them");
		}
		Integer[] integerTemp = iMatchColumns.toArray(new Integer[0]);

		for (int i = 0; i < integerTemp.length; i++) {
			intTemp[i] = (integerTemp[i]).intValue();
		}

		return intTemp;
	}

	/**
	 * Sets the designated parameter to the given int array. This forms the basis of
	 * the join for the <code>JoinRowSet</code> as the column which will form the
	 * basis of the join.
	 * <P>
	 * The parameter value set by this method is stored internally and will be
	 * supplied as the appropriate parameter in this rowset's command when the
	 * method <code>getMatchColumnIndexes</code> is called.
	 *
	 * @param columnIdxes the indexes into this rowset object's internal
	 *                    representation of parameter values; the first parameter is
	 *                    0, the second is 1, and so on; must be <code>0</code> or
	 *                    greater
	 * @throws SQLException if an error occurs or the parameter index is out of
	 *                      bounds
	 */
	public void setMatchColumn(int[] columnIdxes) throws SQLException {

		for (int j = 0; j < columnIdxes.length; j++) {
			if (columnIdxes[j] < 0) {
				throw new SQLException("Match columns should be greater than 0");
			}
		}
		for (int i = 0; i < columnIdxes.length; i++) {
			iMatchColumns.add(i, columnIdxes[i]);
		}
	}

	/**
	 * Sets the designated parameter to the given String array. This forms the basis
	 * of the join for the <code>JoinRowSet</code> as the column which will form the
	 * basis of the join.
	 * <P>
	 * The parameter value set by this method is stored internally and will be
	 * supplied as the appropriate parameter in this rowset's command when the
	 * method <code>getMatchColumn</code> is called.
	 *
	 * @param columnNames the name of the column into this rowset object's internal
	 *                    representation of parameter values
	 * @throws SQLException if an error occurs or the parameter index is out of
	 *                      bounds
	 */
	public void setMatchColumn(String[] columnNames) throws SQLException {

		for (int j = 0; j < columnNames.length; j++) {
			if (columnNames[j] == null || columnNames[j].isEmpty()) {
				throw new SQLException("Match columns should be empty or null string");
			}
		}
		for (int i = 0; i < columnNames.length; i++) {
			strMatchColumns.add(i, columnNames[i]);
		}
	}

	/**
	 * Sets the designated parameter to the given <code>int</code> object. This
	 * forms the basis of the join for the <code>JoinRowSet</code> as the column
	 * which will form the basis of the join.
	 * <P>
	 * The parameter value set by this method is stored internally and will be
	 * supplied as the appropriate parameter in this rowset's command when the
	 * method <code>getMatchColumn</code> is called.
	 *
	 * @param columnIdx the index into this rowset object's internal representation
	 *                  of parameter values; the first parameter is 0, the second is
	 *                  1, and so on; must be <code>0</code> or greater
	 * @throws SQLException if an error occurs or the parameter index is out of
	 *                      bounds
	 */
	public void setMatchColumn(int columnIdx) throws SQLException {
		// validate, if col is ok to be set
		if (columnIdx < 0) {
			throw new SQLException("Match columns should be greater than 0");
		} else {
			iMatchColumns.set(0, columnIdx);
		}
	}

	/**
	 * Sets the designated parameter to the given <code>String</code> object. This
	 * forms the basis of the join for the <code>JoinRowSet</code> as the column
	 * which will form the basis of the join.
	 * <P>
	 * The parameter value set by this method is stored internally and will be
	 * supplied as the appropriate parameter in this rowset's command when the
	 * method <code>getMatchColumn</code> is called.
	 *
	 * @param columnName the name of the column into this rowset object's internal
	 *                   representation of parameter values
	 * @throws SQLException if an error occurs or the parameter index is out of
	 *                      bounds
	 */
	public void setMatchColumn(String columnName) throws SQLException {
		// validate, if col is ok to be set
		if (columnName == null || columnName.trim().isEmpty()) {
			throw new SQLException("Match columns should not be empty or null string");
		} else {
			strMatchColumns.set(0, columnName.trim());
		}
	}

	/**
	 * Unsets the designated parameter to the given <code>int</code> object. This
	 * was set using <code>setMatchColumn</code> as the column which will form the
	 * basis of the join.
	 * <P>
	 * The parameter value unset by this method should be same as was set.
	 *
	 * @param columnIdx the index into this rowset object's internal representation
	 *                  of parameter values
	 * @throws SQLException if an error occurs or the parameter index is out of
	 *                      bounds or if the columnIdx is not the same as set using
	 *                      <code>setMatchColumn(int)</code>
	 */
	public void unsetMatchColumn(int columnIdx) throws SQLException {
		// check if we are unsetting the SAME column
		if (!iMatchColumns.get(0).equals(Integer.valueOf(columnIdx))) {
			throw new SQLException("Columns being unset are not the same as set");
		} else if (strMatchColumns.get(0) != null) {
			throw new SQLException("Use column name as argument to unsetMatchColumn");
		} else {
			// that is, we are unsetting it.
			iMatchColumns.set(0, -1);
		}
	}

	/**
	 * Unsets the designated parameter to the given <code>String</code> object. This
	 * was set using <code>setMatchColumn</code> as the column which will form the
	 * basis of the join.
	 * <P>
	 * The parameter value unset by this method should be same as was set.
	 *
	 * @param columnName the index into this rowset object's internal representation
	 *                   of parameter values
	 * @throws SQLException if an error occurs or the parameter index is out of
	 *                      bounds or if the columnName is not the same as set using
	 *                      <code>setMatchColumn(String)</code>
	 */
	public void unsetMatchColumn(String columnName) throws SQLException {
		// check if we are unsetting the same column
		columnName = columnName.trim();

		if (!((strMatchColumns.get(0)).equals(columnName))) {
			throw new SQLException("Columns being unset are not the same as set");
		} else if (iMatchColumns.get(0) > 0) {
			throw new SQLException("Use column ID as argument to unsetMatchColumn");
		} else {
			strMatchColumns.set(0, null); // that is, we are unsetting it.
		}
	}

	/**
	 * Notifies registered listeners that a RowSet object in the given RowSetEvent
	 * object has populated a number of additional rows. The <code>numRows</code>
	 * parameter ensures that this event will only be fired every
	 * <code>numRow</code>.
	 * <p>
	 * The source of the event can be retrieved with the method event.getSource.
	 *
	 * @param event   a <code>RowSetEvent</code> object that contains the
	 *                <code>RowSet</code> object that is the source of the events
	 * @param numRows when populating, the number of rows interval on which the
	 *                <code>CachedRowSet</code> populated should fire; the default
	 *                value is zero; cannot be less than <code>fetchSize</code> or
	 *                zero
	 */
	public void rowSetPopulated(RowSetEvent event, int numRows) throws SQLException {

		if (numRows < 0 || numRows < getFetchSize()) {
			throw new SQLException("Number of rows is less than zero or less than fetch size");
		}

		if (size() % numRows == 0) {
			notifyRowSetChanged();
		}
	}

	/**
	 * Populates this <code>CachedRowSet</code> object with data from the given
	 * <code>ResultSet</code> object. While related to the
	 * <code>populate(ResultSet)</code> method, an additional parameter is provided
	 * to allow starting position within the <code>ResultSet</code> from where to
	 * populate the CachedRowSet instance.
	 *
	 * This method is an alternative to the method <code>execute</code> for filling
	 * the rowset with data. The method <code>populate</code> does not require that
	 * the properties needed by the method <code>execute</code>, such as the
	 * <code>command</code> property, be set. This is true because the method
	 * <code>populate</code> is given the <code>ResultSet</code> object from which
	 * to get data and thus does not need to use the properties required for setting
	 * up a connection and executing this <code>CachedRowSetImpl</code> object's
	 * command.
	 * <P>
	 * After populating this rowset with data, the method <code>populate</code> sets
	 * the rowset's metadata and then sends a <code>RowSetChangedEvent</code> object
	 * to all registered listeners prior to returning.
	 *
	 * @param data  the <code>ResultSet</code> object containing the data to be read
	 *              into this <code>CachedRowSetImpl</code> object
	 * @param start the integer specifing the position in the <code>ResultSet</code>
	 *              object to popultate the <code>CachedRowSetImpl</code> object.
	 * @throws SQLException if an error occurs; or the max row setting is violated
	 *                      while populating the RowSet.Also id the start position
	 *                      is negative.
	 * @see #execute
	 */
	@Override
	public void populate(ResultSet data, int start) throws SQLException {

		int rowsFetched;
		Row row;
		int numCols;
		int i;
		Map<String, Class<?>> map = getTypeMap();
		Object obj;
		int mRows;

		cursorPos = 0;
		if (populatecallcount == 0) {
			if (start < 0) {
				throw new SQLException("Start position cannot be negative");
			}
			if (getMaxRows() == 0) {
				data.absolute(start);
				while (data.next()) {
					totalRows++;
				}
				totalRows++;
			}
			startPos = start;
		}
		populatecallcount = populatecallcount + 1;
		resultSet = data;
		if ((endPos - startPos) >= getMaxRows() && (getMaxRows() > 0)) {
			endPos = prevEndPos;
			pagenotend = false;
			return;
		}

		if ((maxRowsreached != getMaxRows() || maxRowsreached != totalRows) && pagenotend) {
			startPrev = start - getPageSize();
		}

		if (pageSize == 0) {
			prevEndPos = endPos;
			endPos = start + getMaxRows();
		} else {
			prevEndPos = endPos;
			endPos = start + getPageSize();
		}

		if (start == 1) {
			resultSet.beforeFirst();
		} else {
			resultSet.absolute(start - 1);
		}
		rvh = new ArrayList<>(pageSize == 0 ? getMaxRows() : getPageSize());
		if (data == null) {
			throw new SQLException("Invalid ResultSet object supplied to populate method");
		}

		// get the meta data for this ResultSet
		resultSetMetaData = data.getMetaData();

		// set up the metadata
		rowSetMetaData = new RowSetMetaDataImpl();
		initMetaData(rowSetMetaData, resultSetMetaData);

		// release the meta-data so that aren't tempted to use it.
		resultSetMetaData = null;
		numCols = rowSetMetaData.getColumnCount();
		mRows = this.getMaxRows();
		rowsFetched = 0;

		if (!data.next() && mRows == 0) {
			endPos = prevEndPos;
			pagenotend = false;
			return;
		}

		data.previous();

		while (data.next()) {

			row = new Row(numCols);
			if (pageSize == 0) {
				if (rowsFetched >= mRows && mRows > 0) {
					rowsetWarning.setNextException(new SQLException(MESSAGE_MAX_ROWS_EXCEEDED));
					break;
				}
			} else {
				if ((rowsFetched >= pageSize) || (maxRowsreached >= mRows && mRows > 0)) {
					rowsetWarning.setNextException(new SQLException(MESSAGE_MAX_ROWS_EXCEEDED));
					break;
				}
			}

			for (i = 1; i <= numCols; i++) {
				/*
				 * check if the user has set a map. If no map is set then use plain getObject.
				 * This lets us work with drivers that do not support getObject with a map in
				 * fairly sensible way
				 */
				if (map == null) {
					obj = data.getObject(i);
				} else {
					obj = data.getObject(i, map);
				}
				/*
				 * the following block checks for the various types that we have to serialize in
				 * order to store - right now only structs have been tested
				 */
				if (obj instanceof Struct) {
					obj = new SerialStruct((Struct) obj, map);
				} else if (obj instanceof SQLData) {
					obj = new SerialStruct((SQLData) obj, map);
				} else if (obj instanceof Blob) {
					obj = new SerialBlob((Blob) obj);
				} else if (obj instanceof Clob) {
					obj = new SerialClob((Clob) obj);
				} else if (obj instanceof java.sql.Array) {
					obj = new SerialArray((java.sql.Array) obj, map);
				}

				row.initColumnObject(i, obj);
			}
			rowsFetched++;
			maxRowsreached++;
			rvh.add(row);
		}
		numRows = rowsFetched;
		// Also rowsFetched should be equal to rvh.size()
		// notify any listeners that the rowset has changed
		notifyRowSetChanged();

	}

	/**
	 * The nextPage gets the next page, that is a <code>CachedRowSetImpl</code>
	 * object containing the number of rows specified by page size.
	 * 
	 * @return boolean value true indicating whether there are more pages to come
	 *         and false indicating that this is the last page.
	 * @throws SQLException if an error occurs or this called before calling
	 *                      populate.
	 */
	public boolean nextPage() throws SQLException {

		if (populatecallcount == 0) {
			throw new SQLException("Populate data before calling");
		}
		// Fix for 6554186
		onFirstPage = false;
		populate(resultSet, endPos);
		return pagenotend;
	}

	/**
	 * This is the setter function for setting the size of the page, which specifies
	 * how many rows have to be retrived at a time.
	 *
	 * @param size which is the page size
	 * @throws SQLException if size is less than zero or greater than max rows.
	 */
	public void setPageSize(int size) throws SQLException {
		if (size < 0) {
			throw new SQLException("Page size cannot be less than zero");
		}
		if (size > getMaxRows() && getMaxRows() != 0) {
			throw new SQLException("Page size cannot be greater than maxRows");
		}
		pageSize = size;
	}

	/**
	 * This is the getter function for the size of the page.
	 *
	 * @return an integer that is the page size.
	 */
	public int getPageSize() {
		return pageSize;
	}

	/**
	 * Retrieves the data present in the page prior to the page from where it is
	 * called.
	 * 
	 * @return boolean value true if it retrieves the previous page, flase if it is
	 *         on the first page.
	 * @throws SQLException if it is called before populate is called or ResultSet
	 *                      is of type <code>ResultSet.TYPE_FORWARD_ONLY</code> or
	 *                      if an error occurs.
	 */
	public boolean previousPage() throws SQLException {
		int pS;
		int mR;
		int rem;

		pS = getPageSize();
		mR = maxRowsreached;

		if (populatecallcount == 0) {
			throw new SQLException("Populate data before calling");
		}

		if (!callWithCon && resultSet.getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException("ResultSet is forward only");
		}

		pagenotend = true;

		if (startPrev < startPos) {
			onFirstPage = true;
			return false;
		}

		if (onFirstPage) {
			return false;
		}

		rem = mR % pS;

		if (rem == 0) {
			maxRowsreached -= (2 * pS);
			populate(resultSet, startPrev);
			return true;
		} else {
			maxRowsreached -= (pS + rem);
			populate(resultSet, startPrev);
			return true;
		}
	}

	/**
	 * Sets the status for the row on which the cursor is positioned. The insertFlag
	 * is used to mention the toggle status for this row
	 * 
	 * @param insertFlag if it is true - marks this row as inserted if it is false -
	 *                   marks it as not a newly inserted row
	 * @throws SQLException if an error occurs while doing this operation
	 */
	public void setRowInserted(boolean insertFlag) throws SQLException {

		checkCursor();

		if (onInsertRow)
			throw new SQLException(MESSAGE_INVALID_OPERATION);

		if (insertFlag) {
			((Row) getCurrentRow()).setInserted();
		} else {
			((Row) getCurrentRow()).clearInserted();
		}
	}

	/**
	 * Retrieves the value of the designated <code>SQL XML</code> parameter as a
	 * <code>SQLXML</code> object in the Java programming language.
	 * 
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a SQLXML object that maps an SQL XML value
	 * @throws SQLException if a database access error occurs
	 * @since 1.6
	 */
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Retrieves the value of the designated <code>SQL XML</code> parameter as a
	 * <code>SQLXML</code> object in the Java programming language.
	 * 
	 * @param colName the name of the column from which to retrieve the value
	 * @return a SQLXML object that maps an SQL XML value
	 * @throws SQLException if a database access error occurs
	 */
	public SQLXML getSQLXML(String colName) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a java.sql.RowId object in the Java
	 * programming language.
	 *
	 * @param columnIndex the first column is 1, the second 2, ...
	 * @return the column value if the value is a SQL <code>NULL</code> the value
	 *         returned is <code>null</code>
	 * @throws SQLException if a database access error occurs
	 * @since 1.6
	 */
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a java.sql.RowId object in the Java
	 * programming language.
	 *
	 * @param columnName the name of the column
	 * @return the column value if the value is a SQL <code>NULL</code> the value
	 *         returned is <code>null</code>
	 * @throws SQLException if a database access error occurs
	 * @since 1.6
	 */
	public RowId getRowId(String columnName) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Updates the designated column with a <code>RowId</code> value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead the
	 * {@code updateRow} or {@code insertRow} methods are called to update the
	 * database.
	 *
	 * @param columnIndex the first column is 1, the second 2, ...
	 * @param x           the column value
	 * @throws SQLException if a database access occurs
	 * @since 1.6
	 */
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Updates the designated column with a <code>RowId</code> value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead the
	 * {@code updateRow} or {@code insertRow} methods are called to update the
	 * database.
	 *
	 * @param columnName the name of the column
	 * @param x          the column value
	 * @throws SQLException if a database access occurs
	 * @since 1.6
	 */
	public void updateRowId(String columnName, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Retrieves the holdability of this ResultSet object
	 * 
	 * @return either ResultSet.HOLD_CURSORS_OVER_COMMIT or
	 *         ResultSet.CLOSE_CURSORS_AT_COMMIT
	 * @throws SQLException if a database error occurs
	 * @since 1.6
	 */
	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Retrieves whether this ResultSet object has been closed. A ResultSet is
	 * closed if the method close has been called on it, or if it is automatically
	 * closed.
	 * 
	 * @return true if this ResultSet object is closed; false if it is still open
	 * @throws SQLException if a database access error occurs
	 * @since 1.6
	 */
	public boolean isClosed() throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * This method is used for updating columns that support National Character
	 * sets. It can be used for updating NCHAR,NVARCHAR and LONGNVARCHAR columns.
	 * 
	 * @param columnIndex the first column is 1, the second 2, ...
	 * @param nString     the value for the column to be updated
	 * @throws SQLException if a database access error occurs
	 * @since 1.6
	 */
	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * This method is used for updating columns that support National Character
	 * sets. It can be used for updating NCHAR,NVARCHAR and LONGNVARCHAR columns.
	 * 
	 * @param columnName name of the Column
	 * @param nString    the value for the column to be updated
	 * @throws SQLException if a database access error occurs
	 * @since 1.6
	 */
	public void updateNString(String columnName, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/*
	 * o This method is used for updating SQL <code>NCLOB</code> type that maps to
	 * <code>java.sql.Types.NCLOB</code>
	 * 
	 * @param columnIndex the first column is 1, the second 2, ...
	 * 
	 * @param nClob the value for the column to be updated
	 * 
	 * @throws SQLException if a database access error occurs
	 * 
	 * @since 1.6
	 */
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * This method is used for updating SQL <code>NCLOB</code> type that maps to
	 * <code>java.sql.Types.NCLOB</code>
	 * 
	 * @param columnName name of the column
	 * @param nClob      the value for the column to be updated
	 * @throws SQLException if a database access error occurs
	 * @since 1.6
	 */
	public void updateNClob(String columnName, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>NClob</code> object in the Java
	 * programming language.
	 *
	 * @param i the first column is 1, the second is 2, ...
	 * @return a <code>NClob</code> object representing the SQL <code>NCLOB</code>
	 *         value in the specified column
	 * @exception SQLException if a database access error occurs
	 * @since 1.6
	 */
	public NClob getNClob(int i) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>NClob</code> object in the Java
	 * programming language.
	 *
	 * @param colName the name of the column from which to retrieve the value
	 * @return a <code>NClob</code> object representing the SQL <code>NCLOB</code>
	 *         value in the specified column
	 * @exception SQLException if a database access error occurs
	 * @since 1.6
	 */
	public NClob getNClob(String colName) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
		return null;
	}

	public boolean isWrapperFor(Class<?> interfaces) throws SQLException {
		return false;
	}

	/**
	 * Sets the designated parameter to the given <code>java.sql.SQLXML</code>
	 * object. The driver converts this to an SQL <code>XML</code> value when it
	 * sends it to the database.
	 * 
	 * @param parameterIndex index of the first parameter is 1, the second is 2, ...
	 * @param xmlObject      a <code>SQLXML</code> object that maps an SQL
	 *                       <code>XML</code> value
	 * @throws SQLException if a database access error occurs
	 * @since 1.6
	 */
	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>java.sql.SQLXML</code>
	 * object. The driver converts this to an <code>SQL XML</code> value when it
	 * sends it to the database.
	 * 
	 * @param parameterName the name of the parameter
	 * @param xmlObject     a <code>SQLXML</code> object that maps an
	 *                      <code>SQL XML</code> value
	 * @throws SQLException if a database access error occurs
	 * @since 1.6
	 */
	@Override
	public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>java.sql.RowId</code>
	 * object. The driver converts this to a SQL <code>ROWID</code> value when it
	 * sends it to the database
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x              the parameter value
	 * @throws SQLException if a database access error occurs
	 *
	 * @since 1.6
	 */
	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>java.sql.RowId</code>
	 * object. The driver converts this to a SQL <code>ROWID</code> when it sends it
	 * to the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @throws SQLException if a database access error occurs
	 * @since 1.6
	 */
	@Override
	public void setRowId(String parameterName, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>Reader</code> object. The
	 * <code>Reader</code> reads the data till end-of-file is reached. The driver
	 * does the necessary conversion from Java character format to the national
	 * character set in the database.
	 * 
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>setNCharacterStream</code> which
	 * takes a length parameter.
	 *
	 * @param parameterIndex of the first parameter is 1, the second is 2, ...
	 * @param value          the parameter value
	 * @throws SQLException                    if the driver does not support
	 *                                         national character sets; if the
	 *                                         driver can detect that a data
	 *                                         conversion error could occur ; if a
	 *                                         database access error occurs; or this
	 *                                         method is called on a closed
	 *                                         <code>PreparedStatement</code>
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 * @since 1.6
	 */
	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>java.sql.NClob</code> object. The
	 * object implements the <code>java.sql.NClob</code> interface. This
	 * <code>NClob</code> object maps to a SQL <code>NCLOB</code>.
	 * 
	 * @param parameterName the name of the column to be set
	 * @param value         the parameter value
	 * @throws SQLException if the driver does not support national character sets;
	 *                      if the driver can detect that a data conversion error
	 *                      could occur; or if a database access error occurs
	 * @since 1.6
	 */
	@Override
	public void setNClob(String parameterName, NClob value) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>Reader</code> object. It is intended
	 * for use when accessing <code>NCHAR</code>,<code>NVARCHAR</code> and
	 * <code>LONGNVARCHAR</code> columns.
	 *
	 * @return a <code>Reader</code> object that contains the column value; if the
	 *         value is SQL <code>NULL</code>, the value returned is
	 *         <code>null</code> in the Java programming language.
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @exception SQLException if a database access error occurs
	 * @since 1.6
	 */
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>Reader</code> object. It is intended
	 * for use when accessing <code>NCHAR</code>,<code>NVARCHAR</code> and
	 * <code>LONGNVARCHAR</code> columns.
	 *
	 * @param columnName the name of the column
	 * @return a <code>Reader</code> object that contains the column value; if the
	 *         value is SQL <code>NULL</code>, the value returned is
	 *         <code>null</code> in the Java programming language
	 * @exception SQLException if a database access error occurs
	 * @since 1.6
	 */
	public Reader getNCharacterStream(String columnName) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Updates the designated column with a <code>java.sql.SQLXML</code> value. The
	 * updater methods are used to update column values in the current row or the
	 * insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 * 
	 * @param columnIndex the first column is 1, the second 2, ...
	 * @param xmlObject   the value for the column to be updated
	 * @throws SQLException if a database access error occurs
	 * @since 1.6
	 */
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Updates the designated column with a <code>java.sql.SQLXML</code> value. The
	 * updater methods are used to update column values in the current row or the
	 * insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnName the name of the column
	 * @param xmlObject  the column value
	 * @throws SQLException if a database access occurs
	 * @since 1.6
	 */
	public void updateSQLXML(String columnName, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>String</code> in the Java
	 * programming language. It is intended for use when accessing
	 * <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code>
	 * columns.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return the column value; if the value is SQL <code>NULL</code>, the value
	 *         returned is <code>null</code>
	 * @exception SQLException if a database access error occurs
	 * @since 1.6
	 */
	public String getNString(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * <code>ResultSet</code> object as a <code>String</code> in the Java
	 * programming language. It is intended for use when accessing
	 * <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code>
	 * columns.
	 *
	 * @param columnName the SQL name of the column
	 * @return the column value; if the value is SQL <code>NULL</code>, the value
	 *         returned is <code>null</code>
	 * @exception SQLException if a database access error occurs
	 * @since 1.6
	 */
	public String getNString(String columnName) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Updates the designated column with a character stream value, which will have
	 * the specified number of bytes. The driver does the necessary conversion from
	 * Java character format to the national character set in the database. It is
	 * intended for use when updating NCHAR,NVARCHAR and LONGNVARCHAR columns. The
	 * updater methods are used to update column values in the current row or the
	 * insert row. The updater methods do not update the underlying database;
	 * instead the updateRow or insertRow methods are called to update the database.
	 *
	 * @param columnIndex - the first column is 1, the second is 2, ...
	 * @param x           - the new column value
	 * @param length      - the length of the stream
	 * @exception SQLException if a database access error occurs
	 * @since 1.6
	 */
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Updates the designated column with a character stream value, which will have
	 * the specified number of bytes. The driver does the necessary conversion from
	 * Java character format to the national character set in the database. It is
	 * intended for use when updating NCHAR,NVARCHAR and LONGNVARCHAR columns. The
	 * updater methods are used to update column values in the current row or the
	 * insert row. The updater methods do not update the underlying database;
	 * instead the updateRow or insertRow methods are called to update the database.
	 *
	 * @param columnName - name of the Column
	 * @param x          - the new column value
	 * @param length     - the length of the stream
	 * @exception SQLException if a database access error occurs
	 * @since 1.6
	 */
	public void updateNCharacterStream(String columnName, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_OPERATION_NOT_YET_SUPPORTED);
	}

	/**
	 * Updates the designated column with a character stream value. The driver does
	 * the necessary conversion from Java character format to the national character
	 * set in the database. It is intended for use when updating
	 * <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code>
	 * columns.
	 *
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateNCharacterStream</code>
	 * which takes a length parameter.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param x           the new column value
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column with a character stream value. The driver does
	 * the necessary conversion from Java character format to the national character
	 * set in the database. It is intended for use when updating
	 * <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code>
	 * columns.
	 *
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateNCharacterStream</code>
	 * which takes a length parameter.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the la bel is
	 *                    the name of the column
	 * @param reader      the <code>Reader</code> object containing the new column
	 *                    value
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column using the given input stream, which will have
	 * the specified number of bytes. When a very large ASCII value is input to a
	 * <code>LONGVARCHAR</code> parameter, it may be more practical to send it via a
	 * <code>InputStream</code>. Data will be read from the stream as needed until
	 * end-of-file is reached. The JDBC driver will do any necessary conversion from
	 * ASCII to the database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <p>
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param inputStream An object that contains the data to set the parameter
	 *                    value to.
	 * @param length      the number of bytes in the parameter data.
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column using the given input stream, which will have
	 * the specified number of bytes. When a very large ASCII value is input to a
	 * <code>LONGVARCHAR</code> parameter, it may be more practical to send it via a
	 * <code>InputStream</code>. Data will be read from the stream as needed until
	 * end-of-file is reached. The JDBC driver will do any necessary conversion from
	 * ASCII to the database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <p>
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the label is
	 *                    the name of the column
	 * @param inputStream An object that contains the data to set the parameter
	 *                    value to.
	 * @param length      the number of bytes in the parameter data.
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column using the given input stream. When a very large
	 * ASCII value is input to a <code>LONGVARCHAR</code> parameter, it may be more
	 * practical to send it via a <code>InputStream</code>. Data will be read from
	 * the stream as needed until end-of-file is reached. The JDBC driver will do
	 * any necessary conversion from ASCII to the database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateBlob</code> which takes a
	 * length parameter.
	 * <p>
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param inputStream An object that contains the data to set the parameter
	 *                    value to.
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column using the given input stream. When a very large
	 * ASCII value is input to a <code>LONGVARCHAR</code> parameter, it may be more
	 * practical to send it via a <code>InputStream</code>. Data will be read from
	 * the stream as needed until end-of-file is reached. The JDBC driver will do
	 * any necessary conversion from ASCII to the database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateBlob</code> which takes a
	 * length parameter.
	 * <p>
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the la bel is
	 *                    the name of the column
	 * @param inputStream An object that contains the data to set the parameter
	 *                    value to.
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column using the given <code>Reader</code> object,
	 * which is the given number of characters long. When a very large UNICODE value
	 * is input to a <code>LONGVARCHAR</code> parameter, it may be more practical to
	 * send it via a <code>Reader</code> object. The data will be read from the
	 * stream as needed until end-of-file is reached. The JDBC driver will do any
	 * necessary conversion from UNICODE to the database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <p>
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param reader      An object that contains the data to set the parameter
	 *                    value to.
	 * @param length      the number of characters in the parameter data.
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column using the given <code>Reader</code> object,
	 * which is the given number of characters long. When a very large UNICODE value
	 * is input to a <code>LONGVARCHAR</code> parameter, it may be more practical to
	 * send it via a <code>Reader</code> object. The data will be read from the
	 * stream as needed until end-of-file is reached. The JDBC driver will do any
	 * necessary conversion from UNICODE to the database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <p>
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the label is
	 *                    the name of the column
	 * @param reader      An object that contains the data to set the parameter
	 *                    value to.
	 * @param length      the number of characters in the parameter data.
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column using the given <code>Reader</code> object.
	 * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
	 * parameter, it may be more practical to send it via a <code>Reader</code>
	 * object. The data will be read from the stream as needed until end-of-file is
	 * reached. The JDBC driver will do any necessary conversion from UNICODE to the
	 * database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateClob</code> which takes a
	 * length parameter.
	 * <p>
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param reader      An object that contains the data to set the parameter
	 *                    value to.
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column using the given <code>Reader</code> object.
	 * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
	 * parameter, it may be more practical to send it via a <code>Reader</code>
	 * object. The data will be read from the stream as needed until end-of-file is
	 * reached. The JDBC driver will do any necessary conversion from UNICODE to the
	 * database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateClob</code> which takes a
	 * length parameter.
	 * <p>
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the la bel is
	 *                    the name of the column
	 * @param reader      An object that contains the data to set the parameter
	 *                    value to.
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column using the given <code>Reader</code> object,
	 * which is the given number of characters long. When a very large UNICODE value
	 * is input to a <code>LONGVARCHAR</code> parameter, it may be more practical to
	 * send it via a <code>Reader</code> object. The data will be read from the
	 * stream as needed until end-of-file is reached. The JDBC driver will do any
	 * necessary conversion from UNICODE to the database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <p>
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnIndex the first column is 1, the second 2, ...
	 * @param reader      An object that contains the data to set the parameter
	 *                    value to.
	 * @param length      the number of characters in the parameter data.
	 * @throws SQLException if the driver does not support national character sets;
	 *                      if the driver can detect that a data conversion error
	 *                      could occur; this method is called on a closed result
	 *                      set, if a database access error occurs or the result set
	 *                      concurrency is <code>CONCUR_READ_ONLY</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column using the given <code>Reader</code> object,
	 * which is the given number of characters long. When a very large UNICODE value
	 * is input to a <code>LONGVARCHAR</code> parameter, it may be more practical to
	 * send it via a <code>Reader</code> object. The data will be read from the
	 * stream as needed until end-of-file is reached. The JDBC driver will do any
	 * necessary conversion from UNICODE to the database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <p>
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the label is
	 *                    the name of the column
	 * @param reader      An object that contains the data to set the parameter
	 *                    value to.
	 * @param length      the number of characters in the parameter data.
	 * @throws SQLException if the driver does not support national character sets;
	 *                      if the driver can detect that a data conversion error
	 *                      could occur; this method is called on a closed result
	 *                      set; if a database access error occurs or the result set
	 *                      concurrency is <code>CONCUR_READ_ONLY</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column using the given <code>Reader</code> object.
	 * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
	 * parameter, it may be more practical to send it via a <code>Reader</code>
	 * object. The data will be read from the stream as needed until end-of-file is
	 * reached. The JDBC driver will do any necessary conversion from UNICODE to the
	 * database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateNClob</code> which takes a
	 * length parameter.
	 * <p>
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnIndex the first column is 1, the second 2, ...
	 * @param reader      An object that contains the data to set the parameter
	 *                    value to.
	 * @throws SQLException if the driver does not support national character sets;
	 *                      if the driver can detect that a data conversion error
	 *                      could occur; this method is called on a closed result
	 *                      set, if a database access error occurs or the result set
	 *                      concurrency is <code>CONCUR_READ_ONLY</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column using the given <code>Reader</code> object.
	 * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
	 * parameter, it may be more practical to send it via a <code>Reader</code>
	 * object. The data will be read from the stream as needed until end-of-file is
	 * reached. The JDBC driver will do any necessary conversion from UNICODE to the
	 * database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateNClob</code> which takes a
	 * length parameter.
	 * <p>
	 * The updater methods are used to update column values in the current row or
	 * the insert row. The updater methods do not update the underlying database;
	 * instead the <code>updateRow</code> or <code>insertRow</code> methods are
	 * called to update the database.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the la bel is
	 *                    the name of the column
	 * @param reader      An object that contains the data to set the parameter
	 *                    value to.
	 * @throws SQLException if the driver does not support national character sets;
	 *                      if the driver can detect that a data conversion error
	 *                      could occur; this method is called on a closed result
	 *                      set; if a database access error occurs or the result set
	 *                      concurrency is <code>CONCUR_READ_ONLY</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column with an ascii stream value, which will have the
	 * specified number of bytes. The updater methods are used to update column
	 * values in the current row or the insert row. The updater methods do not
	 * update the underlying database; instead the <code>updateRow</code> or
	 * <code>insertRow</code> methods are called to update the database.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param x           the new column value
	 * @param length      the length of the stream
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		// ignore
	}

	/**
	 * Updates the designated column with a binary stream value, which will have the
	 * specified number of bytes. The updater methods are used to update column
	 * values in the current row or the insert row. The updater methods do not
	 * update the underlying database; instead the <code>updateRow</code> or
	 * <code>insertRow</code> methods are called to update the database.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param x           the new column value
	 * @param length      the length of the stream
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		// ignore
	}

	/**
	 * Updates the designated column with a character stream value, which will have
	 * the specified number of bytes. The updater methods are used to update column
	 * values in the current row or the insert row. The updater methods do not
	 * update the underlying database; instead the <code>updateRow</code> or
	 * <code>insertRow</code> methods are called to update the database.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param x           the new column value
	 * @param length      the length of the stream
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column with a character stream value, which will have
	 * the specified number of bytes. The updater methods are used to update column
	 * values in the current row or the insert row. The updater methods do not
	 * update the underlying database; instead the <code>updateRow</code> or
	 * <code>insertRow</code> methods are called to update the database.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the la bel is
	 *                    the name of the column
	 * @param reader      the <code>Reader</code> object containing the new column
	 *                    value
	 * @param length      the length of the stream
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column with an ascii stream value, which will have the
	 * specified number of bytes.. The updater methods are used to update column
	 * values in the current row or the insert row. The updater methods do not
	 * update the underlying database; instead the <code>updateRow</code> or
	 * <code>insertRow</code> methods are called to update the database.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the label is
	 *                    the name of the column
	 * @param x           the new column value
	 * @param length      the length of the stream
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		// ignore
	}

	/**
	 * Updates the designated column with a binary stream value, which will have the
	 * specified number of bytes. The updater methods are used to update column
	 * values in the current row or the insert row. The updater methods do not
	 * update the underlying database; instead the <code>updateRow</code> or
	 * <code>insertRow</code> methods are called to update the database.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the label is
	 *                    the name of the column
	 * @param x           the new column value
	 * @param length      the length of the stream
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		// ignore
	}

	/**
	 * Updates the designated column with a binary stream value. The updater methods
	 * are used to update column values in the current row or the insert row. The
	 * updater methods do not update the underlying database; instead the
	 * <code>updateRow</code> or <code>insertRow</code> methods are called to update
	 * the database.
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateBinaryStream</code> which
	 * takes a length parameter.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param x           the new column value
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column with a binary stream value. The updater methods
	 * are used to update column values in the current row or the insert row. The
	 * updater methods do not update the underlying database; instead the
	 * <code>updateRow</code> or <code>insertRow</code> methods are called to update
	 * the database.
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateBinaryStream</code> which
	 * takes a length parameter.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the la bel is
	 *                    the name of the column
	 * @param x           the new column value
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column with a character stream value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead the
	 * <code>updateRow</code> or <code>insertRow</code> methods are called to update
	 * the database.
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateCharacterStream</code>
	 * which takes a length parameter.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param x           the new column value
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column with a character stream value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead the
	 * <code>updateRow</code> or <code>insertRow</code> methods are called to update
	 * the database.
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateCharacterStream</code>
	 * which takes a length parameter.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the la bel is
	 *                    the name of the column
	 * @param reader      the <code>Reader</code> object containing the new column
	 *                    value
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column with an ascii stream value. The updater methods
	 * are used to update column values in the current row or the insert row. The
	 * updater methods do not update the underlying database; instead the
	 * <code>updateRow</code> or <code>insertRow</code> methods are called to update
	 * the database.
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateAsciiStream</code> which
	 * takes a length parameter.
	 *
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param x           the new column value
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Updates the designated column with an ascii stream value. The updater methods
	 * are used to update column values in the current row or the insert row. The
	 * updater methods do not update the underlying database; instead the
	 * <code>updateRow</code> or <code>insertRow</code> methods are called to update
	 * the database.
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>updateAsciiStream</code> which
	 * takes a length parameter.
	 *
	 * @param columnLabel the label for the column specified with the SQL AS clause.
	 *                    If the SQL AS clause was not specified, then the la bel is
	 *                    the name of the column
	 * @param x           the new column value
	 * @exception SQLException                    if a database access error occurs,
	 *                                            the result set concurrency is
	 *                                            <code>CONCUR_READ_ONLY</code> or
	 *                                            this method is called on a closed
	 *                                            result set
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		// ignore
	}

	/**
	 * Sets the designated parameter to the given <code>java.net.URL</code> value.
	 * The driver converts this to an SQL <code>DATALINK</code> value when it sends
	 * it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x              the <code>java.net.URL</code> object to be set
	 * @exception SQLException if a database access error occurs or this method is
	 *                         called on a closed <code>PreparedStatement</code>
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 * @since 1.4
	 */
	@Override
	public void setURL(int parameterIndex, java.net.URL x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>Reader</code> object. This method
	 * differs from the <code>setCharacterStream (int, Reader)</code> method because
	 * it informs the driver that the parameter value should be sent to the server
	 * as a <code>NCLOB</code>. When the <code>setCharacterStream</code> method is
	 * used, the driver may have to do extra work to determine whether the parameter
	 * data should be sent to the server as a <code>LONGNVARCHAR</code> or a
	 * <code>NCLOB</code>
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>setNClob</code> which takes a
	 * length parameter.
	 *
	 * @param parameterIndex index of the first parameter is 1, the second is 2, ...
	 * @param reader         An object that contains the data to set the parameter
	 *                       value to.
	 * @throws SQLException                    if parameterIndex does not correspond
	 *                                         to a parameter marker in the SQL
	 *                                         statement; if the driver does not
	 *                                         support national character sets; if
	 *                                         the driver can detect that a data
	 *                                         conversion error could occur; if a
	 *                                         database access error occurs or this
	 *                                         method is called on a closed
	 *                                         <code>PreparedStatement</code>
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 *
	 * @since 1.6
	 */
	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>Reader</code> object. The
	 * <code>reader</code> must contain the number of characters specified by length
	 * otherwise a <code>SQLException</code> will be generated when the
	 * <code>CallableStatement</code> is executed. This method differs from the
	 * <code>setCharacterStream (int, Reader, int)</code> method because it informs
	 * the driver that the parameter value should be sent to the server as a
	 * <code>NCLOB</code>. When the <code>setCharacterStream</code> method is used,
	 * the driver may have to do extra work to determine whether the parameter data
	 * should be send to the server as a <code>LONGNVARCHAR</code> or a
	 * <code>NCLOB</code>
	 *
	 * @param parameterName the name of the parameter to be set
	 * @param reader        An object that contains the data to set the parameter
	 *                      value to.
	 * @param length        the number of characters in the parameter data.
	 * @throws SQLException if parameterIndex does not correspond to a parameter
	 *                      marker in the SQL statement; if the length specified is
	 *                      less than zero; if the driver does not support national
	 *                      character sets; if the driver can detect that a data
	 *                      conversion error could occur; if a database access error
	 *                      occurs or this method is called on a closed
	 *                      <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	@Override
	public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>Reader</code> object. This method
	 * differs from the <code>setCharacterStream (int, Reader)</code> method because
	 * it informs the driver that the parameter value should be sent to the server
	 * as a <code>NCLOB</code>. When the <code>setCharacterStream</code> method is
	 * used, the driver may have to do extra work to determine whether the parameter
	 * data should be send to the server as a <code>LONGNVARCHAR</code> or a
	 * <code>NCLOB</code>
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>setNClob</code> which takes a
	 * length parameter.
	 *
	 * @param parameterName the name of the parameter
	 * @param reader        An object that contains the data to set the parameter
	 *                      value to.
	 * @throws SQLException                    if the driver does not support
	 *                                         national character sets; if the
	 *                                         driver can detect that a data
	 *                                         conversion error could occur; if a
	 *                                         database access error occurs or this
	 *                                         method is called on a closed
	 *                                         <code>CallableStatement</code>
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 *
	 * @since 1.6
	 */
	@Override
	public void setNClob(String parameterName, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>Reader</code> object. The reader
	 * must contain the number of characters specified by length otherwise a
	 * <code>SQLException</code> will be generated when the
	 * <code>PreparedStatement</code> is executed. This method differs from the
	 * <code>setCharacterStream (int, Reader, int)</code> method because it informs
	 * the driver that the parameter value should be sent to the server as a
	 * <code>NCLOB</code>. When the <code>setCharacterStream</code> method is used,
	 * the driver may have to do extra work to determine whether the parameter data
	 * should be sent to the server as a <code>LONGNVARCHAR</code> or a
	 * <code>NCLOB</code>
	 * 
	 * @param parameterIndex index of the first parameter is 1, the second is 2, ...
	 * @param reader         An object that contains the data to set the parameter
	 *                       value to.
	 * @param length         the number of characters in the parameter data.
	 * @throws SQLException                    if parameterIndex does not correspond
	 *                                         to a parameter marker in the SQL
	 *                                         statement; if the length specified is
	 *                                         less than zero; if the driver does
	 *                                         not support national character sets;
	 *                                         if the driver can detect that a data
	 *                                         conversion error could occur; if a
	 *                                         database access error occurs or this
	 *                                         method is called on a closed
	 *                                         <code>PreparedStatement</code>
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 *
	 * @since 1.6
	 */
	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>java.sql.NClob</code> object. The
	 * driver converts this to a SQL <code>NCLOB</code> value when it sends it to
	 * the database.
	 * 
	 * @param parameterIndex of the first parameter is 1, the second is 2, ...
	 * @param value          the parameter value
	 * @throws SQLException if the driver does not support national character sets;
	 *                      if the driver can detect that a data conversion error
	 *                      could occur ; or if a database access error occurs
	 * @since 1.6
	 */
	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>String</code> object. The
	 * driver converts this to a SQL <code>NCHAR</code> or <code>NVARCHAR</code> or
	 * <code>LONGNVARCHAR</code> value (depending on the argument's size relative to
	 * the driver's limits on <code>NVARCHAR</code> values) when it sends it to the
	 * database.
	 *
	 * @param parameterIndex of the first parameter is 1, the second is 2, ...
	 * @param value          the parameter value
	 * @throws SQLException if the driver does not support national character sets;
	 *                      if the driver can detect that a data conversion error
	 *                      could occur ; or if a database access error occurs
	 * @since 1.6
	 */
	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>String</code> object. The
	 * driver converts this to a SQL <code>NCHAR</code> or <code>NVARCHAR</code> or
	 * <code>LONGNVARCHAR</code>
	 * 
	 * @param parameterName the name of the column to be set
	 * @param value         the parameter value
	 * @throws SQLException if the driver does not support national character sets;
	 *                      if the driver can detect that a data conversion error
	 *                      could occur; or if a database access error occurs
	 * @since 1.6
	 */
	@Override
	public void setNString(String parameterName, String value) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>Reader</code> object. The
	 * <code>Reader</code> reads the data till end-of-file is reached. The driver
	 * does the necessary conversion from Java character format to the national
	 * character set in the database.
	 * 
	 * @param parameterIndex of the first parameter is 1, the second is 2, ...
	 * @param value          the parameter value
	 * @param length         the number of characters in the parameter data.
	 * @throws SQLException if the driver does not support national character sets;
	 *                      if the driver can detect that a data conversion error
	 *                      could occur ; or if a database access error occurs
	 * @since 1.6
	 */
	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>Reader</code> object. The
	 * <code>Reader</code> reads the data till end-of-file is reached. The driver
	 * does the necessary conversion from Java character format to the national
	 * character set in the database.
	 * 
	 * @param parameterName the name of the column to be set
	 * @param value         the parameter value
	 * @param length        the number of characters in the parameter data.
	 * @throws SQLException if the driver does not support national character sets;
	 *                      if the driver can detect that a data conversion error
	 *                      could occur; or if a database access error occurs
	 * @since 1.6
	 */
	@Override
	public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>Reader</code> object. The
	 * <code>Reader</code> reads the data till end-of-file is reached. The driver
	 * does the necessary conversion from Java character format to the national
	 * character set in the database.
	 * 
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>setNCharacterStream</code> which
	 * takes a length parameter.
	 *
	 * @param parameterName the name of the parameter
	 * @param value         the parameter value
	 * @throws SQLException                    if the driver does not support
	 *                                         national character sets; if the
	 *                                         driver can detect that a data
	 *                                         conversion error could occur ; if a
	 *                                         database access error occurs; or this
	 *                                         method is called on a closed
	 *                                         <code>CallableStatement</code>
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 * @since 1.6
	 */
	@Override
	public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>java.sql.Timestamp</code>
	 * value, using the given <code>Calendar</code> object. The driver uses the
	 * <code>Calendar</code> object to construct an SQL <code>TIMESTAMP</code>
	 * value, which the driver then sends to the database. With a a
	 * <code>Calendar</code> object, the driver can calculate the timestamp taking
	 * into account a custom timezone. If no <code>Calendar</code> object is
	 * specified, the driver uses the default timezone, which is that of the virtual
	 * machine running the application.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @param cal           the <code>Calendar</code> object the driver will use to
	 *                      construct the timestamp
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getTimestamp
	 * @since 1.4
	 */
	@Override
	public void setTimestamp(String parameterName, java.sql.Timestamp x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>Reader</code> object. The
	 * <code>reader</code> must contain the number of characters specified by length
	 * otherwise a <code>SQLException</code> will be generated when the
	 * <code>CallableStatement</code> is executed. This method differs from the
	 * <code>setCharacterStream (int, Reader, int)</code> method because it informs
	 * the driver that the parameter value should be sent to the server as a
	 * <code>CLOB</code>. When the <code>setCharacterStream</code> method is used,
	 * the driver may have to do extra work to determine whether the parameter data
	 * should be send to the server as a <code>LONGVARCHAR</code> or a
	 * <code>CLOB</code>
	 * 
	 * @param parameterName the name of the parameter to be set
	 * @param reader        An object that contains the data to set the parameter
	 *                      value to.
	 * @param length        the number of characters in the parameter data.
	 * @throws SQLException if parameterIndex does not correspond to a parameter
	 *                      marker in the SQL statement; if the length specified is
	 *                      less than zero; a database access error occurs or this
	 *                      method is called on a closed
	 *                      <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 *
	 * @since 1.6
	 */
	@Override
	public void setClob(String parameterName, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>java.sql.Clob</code> object.
	 * The driver converts this to an SQL <code>CLOB</code> value when it sends it
	 * to the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             a <code>Clob</code> object that maps an SQL
	 *                      <code>CLOB</code> value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	@Override
	public void setClob(String parameterName, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>Reader</code> object. This method
	 * differs from the <code>setCharacterStream (int, Reader)</code> method because
	 * it informs the driver that the parameter value should be sent to the server
	 * as a <code>CLOB</code>. When the <code>setCharacterStream</code> method is
	 * used, the driver may have to do extra work to determine whether the parameter
	 * data should be send to the server as a <code>LONGVARCHAR</code> or a
	 * <code>CLOB</code>
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>setClob</code> which takes a
	 * length parameter.
	 *
	 * @param parameterName the name of the parameter
	 * @param reader        An object that contains the data to set the parameter
	 *                      value to.
	 * @throws SQLException                    if a database access error occurs or
	 *                                         this method is called on a closed
	 *                                         <code>CallableStatement</code>
	 *
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 * @since 1.6
	 */
	@Override
	public void setClob(String parameterName, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>java.sql.Date</code> value
	 * using the default time zone of the virtual machine that is running the
	 * application. The driver converts this to an SQL <code>DATE</code> value when
	 * it sends it to the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getDate
	 * @since 1.4
	 */
	@Override
	public void setDate(String parameterName, java.sql.Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>java.sql.Date</code> value,
	 * using the given <code>Calendar</code> object. The driver uses the
	 * <code>Calendar</code> object to construct an SQL <code>DATE</code> value,
	 * which the driver then sends to the database. With a a <code>Calendar</code>
	 * object, the driver can calculate the date taking into account a custom
	 * timezone. If no <code>Calendar</code> object is specified, the driver uses
	 * the default timezone, which is that of the virtual machine running the
	 * application.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @param cal           the <code>Calendar</code> object the driver will use to
	 *                      construct the date
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getDate
	 * @since 1.4
	 */
	@Override
	public void setDate(String parameterName, java.sql.Date x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>java.sql.Time</code> value.
	 * The driver converts this to an SQL <code>TIME</code> value when it sends it
	 * to the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getTime
	 * @since 1.4
	 */
	@Override
	public void setTime(String parameterName, java.sql.Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>java.sql.Time</code> value,
	 * using the given <code>Calendar</code> object. The driver uses the
	 * <code>Calendar</code> object to construct an SQL <code>TIME</code> value,
	 * which the driver then sends to the database. With a a <code>Calendar</code>
	 * object, the driver can calculate the time taking into account a custom
	 * timezone. If no <code>Calendar</code> object is specified, the driver uses
	 * the default timezone, which is that of the virtual machine running the
	 * application.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @param cal           the <code>Calendar</code> object the driver will use to
	 *                      construct the time
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getTime
	 * @since 1.4
	 */
	@Override
	public void setTime(String parameterName, java.sql.Time x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>Reader</code> object. This method
	 * differs from the <code>setCharacterStream (int, Reader)</code> method because
	 * it informs the driver that the parameter value should be sent to the server
	 * as a <code>CLOB</code>. When the <code>setCharacterStream</code> method is
	 * used, the driver may have to do extra work to determine whether the parameter
	 * data should be sent to the server as a <code>LONGVARCHAR</code> or a
	 * <code>CLOB</code>
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>setClob</code> which takes a
	 * length parameter.
	 *
	 * @param parameterIndex index of the first parameter is 1, the second is 2, ...
	 * @param reader         An object that contains the data to set the parameter
	 *                       value to.
	 * @throws SQLException                    if a database access error occurs,
	 *                                         this method is called on a closed
	 *                                         <code>PreparedStatement</code>or if
	 *                                         parameterIndex does not correspond to
	 *                                         a parameter marker in the SQL
	 *                                         statement
	 *
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 * @since 1.6
	 */
	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>Reader</code> object. The reader
	 * must contain the number of characters specified by length otherwise a
	 * <code>SQLException</code> will be generated when the
	 * <code>PreparedStatement</code> is executed. This method differs from the
	 * <code>setCharacterStream (int, Reader, int)</code> method because it informs
	 * the driver that the parameter value should be sent to the server as a
	 * <code>CLOB</code>. When the <code>setCharacterStream</code> method is used,
	 * the driver may have to do extra work to determine whether the parameter data
	 * should be sent to the server as a <code>LONGVARCHAR</code> or a
	 * <code>CLOB</code>
	 * 
	 * @param parameterIndex index of the first parameter is 1, the second is 2, ...
	 * @param reader         An object that contains the data to set the parameter
	 *                       value to.
	 * @param length         the number of characters in the parameter data.
	 * @throws SQLException                    if a database access error occurs,
	 *                                         this method is called on a closed
	 *                                         <code>PreparedStatement</code>, if
	 *                                         parameterIndex does not correspond to
	 *                                         a parameter marker in the SQL
	 *                                         statement, or if the length specified
	 *                                         is less than zero.
	 *
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 * @since 1.6
	 */
	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>InputStream</code> object. The
	 * inputstream must contain the number of characters specified by length
	 * otherwise a <code>SQLException</code> will be generated when the
	 * <code>PreparedStatement</code> is executed. This method differs from the
	 * <code>setBinaryStream (int, InputStream, int)</code> method because it
	 * informs the driver that the parameter value should be sent to the server as a
	 * <code>BLOB</code>. When the <code>setBinaryStream</code> method is used, the
	 * driver may have to do extra work to determine whether the parameter data
	 * should be sent to the server as a <code>LONGVARBINARY</code> or a
	 * <code>BLOB</code>
	 * 
	 * @param parameterIndex index of the first parameter is 1, the second is 2, ...
	 * @param inputStream    An object that contains the data to set the parameter
	 *                       value to.
	 * @param length         the number of bytes in the parameter data.
	 * @throws SQLException                    if a database access error occurs,
	 *                                         this method is called on a closed
	 *                                         <code>PreparedStatement</code>, if
	 *                                         parameterIndex does not correspond to
	 *                                         a parameter marker in the SQL
	 *                                         statement, if the length specified is
	 *                                         less than zero or if the number of
	 *                                         bytes in the inputstream does not
	 *                                         match the specified length.
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 *
	 * @since 1.6
	 */
	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>InputStream</code> object. This
	 * method differs from the <code>setBinaryStream (int, InputStream)</code>
	 * method because it informs the driver that the parameter value should be sent
	 * to the server as a <code>BLOB</code>. When the <code>setBinaryStream</code>
	 * method is used, the driver may have to do extra work to determine whether the
	 * parameter data should be sent to the server as a <code>LONGVARBINARY</code>
	 * or a <code>BLOB</code>
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>setBlob</code> which takes a
	 * length parameter.
	 *
	 * @param parameterIndex index of the first parameter is 1, the second is 2, ...
	 * @param inputStream    An object that contains the data to set the parameter
	 *                       value to.
	 * @throws SQLException                    if a database access error occurs,
	 *                                         this method is called on a closed
	 *                                         <code>PreparedStatement</code> or if
	 *                                         parameterIndex does not correspond to
	 *                                         a parameter marker in the SQL
	 *                                         statement,
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 *
	 * @since 1.6
	 */
	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>InputStream</code> object. The
	 * <code>inputstream</code> must contain the number of characters specified by
	 * length, otherwise a <code>SQLException</code> will be generated when the
	 * <code>CallableStatement</code> is executed. This method differs from the
	 * <code>setBinaryStream (int, InputStream, int)</code> method because it
	 * informs the driver that the parameter value should be sent to the server as a
	 * <code>BLOB</code>. When the <code>setBinaryStream</code> method is used, the
	 * driver may have to do extra work to determine whether the parameter data
	 * should be sent to the server as a <code>LONGVARBINARY</code> or a
	 * <code>BLOB</code>
	 *
	 * @param parameterName the name of the parameter to be set the second is 2, ...
	 *
	 * @param inputStream   An object that contains the data to set the parameter
	 *                      value to.
	 * @param length        the number of bytes in the parameter data.
	 * @throws SQLException if parameterIndex does not correspond to a parameter
	 *                      marker in the SQL statement, or if the length specified
	 *                      is less than zero; if the number of bytes in the
	 *                      inputstream does not match the specified length; if a
	 *                      database access error occurs or this method is called on
	 *                      a closed <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 *
	 * @since 1.6
	 */
	@Override
	public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>java.sql.Blob</code> object.
	 * The driver converts this to an SQL <code>BLOB</code> value when it sends it
	 * to the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             a <code>Blob</code> object that maps an SQL
	 *                      <code>BLOB</code> value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.6
	 */
	@Override
	public void setBlob(String parameterName, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to a <code>InputStream</code> object. This
	 * method differs from the <code>setBinaryStream (int, InputStream)</code>
	 * method because it informs the driver that the parameter value should be sent
	 * to the server as a <code>BLOB</code>. When the <code>setBinaryStream</code>
	 * method is used, the driver may have to do extra work to determine whether the
	 * parameter data should be send to the server as a <code>LONGVARBINARY</code>
	 * or a <code>BLOB</code>
	 *
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>setBlob</code> which takes a
	 * length parameter.
	 *
	 * @param parameterName the name of the parameter
	 * @param inputStream   An object that contains the data to set the parameter
	 *                      value to.
	 * @throws SQLException                    if a database access error occurs or
	 *                                         this method is called on a closed
	 *                                         <code>CallableStatement</code>
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 *
	 * @since 1.6
	 */
	@Override
	public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the value of the designated parameter with the given object. The second
	 * argument must be an object type; for integral values, the
	 * <code>java.lang</code> equivalent objects should be used.
	 *
	 * <p>
	 * The given Java object will be converted to the given targetSqlType before
	 * being sent to the database.
	 *
	 * If the object has a custom mapping (is of a class implementing the interface
	 * <code>SQLData</code>), the JDBC driver should call the method
	 * <code>SQLData.writeSQL</code> to write it to the SQL data stream. If, on the
	 * other hand, the object is of a class implementing <code>Ref</code>,
	 * <code>Blob</code>, <code>Clob</code>, <code>NClob</code>,
	 * <code>Struct</code>, <code>java.net.URL</code>, or <code>Array</code>, the
	 * driver should pass it to the database as a value of the corresponding SQL
	 * type.
	 * <P>
	 * Note that this method may be used to pass datatabase- specific abstract data
	 * types.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the object containing the input parameter value
	 * @param targetSqlType the SQL type (as defined in java.sql.Types) to be sent
	 *                      to the database. The scale argument may further qualify
	 *                      this type.
	 * @param scale         for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC
	 *                      types, this is the number of digits after the decimal
	 *                      point. For all other types, this value will be ignored.
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if <code>targetSqlType</code> is a
	 *                                            <code>ARRAY</code>,
	 *                                            <code>BLOB</code>,
	 *                                            <code>CLOB</code>,
	 *                                            <code>DATALINK</code>,
	 *                                            <code>JAVA_OBJECT</code>,
	 *                                            <code>NCHAR</code>,
	 *                                            <code>NCLOB</code>,
	 *                                            <code>NVARCHAR</code>,
	 *                                            <code>LONGNVARCHAR</code>,
	 *                                            <code>REF</code>,
	 *                                            <code>ROWID</code>,
	 *                                            <code>SQLXML</code> or
	 *                                            <code>STRUCT</code> data type and
	 *                                            the JDBC driver does not support
	 *                                            this data type
	 * @see Types
	 * @see #getObject
	 * @since 1.4
	 */
	@Override
	public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the value of the designated parameter with the given object. This method
	 * is like the method <code>setObject</code> above, except that it assumes a
	 * scale of zero.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the object containing the input parameter value
	 * @param targetSqlType the SQL type (as defined in java.sql.Types) to be sent
	 *                      to the database
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if <code>targetSqlType</code> is a
	 *                                            <code>ARRAY</code>,
	 *                                            <code>BLOB</code>,
	 *                                            <code>CLOB</code>,
	 *                                            <code>DATALINK</code>,
	 *                                            <code>JAVA_OBJECT</code>,
	 *                                            <code>NCHAR</code>,
	 *                                            <code>NCLOB</code>,
	 *                                            <code>NVARCHAR</code>,
	 *                                            <code>LONGNVARCHAR</code>,
	 *                                            <code>REF</code>,
	 *                                            <code>ROWID</code>,
	 *                                            <code>SQLXML</code> or
	 *                                            <code>STRUCT</code> data type and
	 *                                            the JDBC driver does not support
	 *                                            this data type
	 * @see #getObject
	 * @since 1.4
	 */
	@Override
	public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the value of the designated parameter with the given object. The second
	 * parameter must be of type <code>Object</code>; therefore, the
	 * <code>java.lang</code> equivalent objects should be used for built-in types.
	 *
	 * <p>
	 * The JDBC specification specifies a standard mapping from Java
	 * <code>Object</code> types to SQL types. The given argument will be converted
	 * to the corresponding SQL type before being sent to the database.
	 *
	 * <p>
	 * Note that this method may be used to pass datatabase- specific abstract data
	 * types, by using a driver-specific Java type.
	 *
	 * If the object is of a class implementing the interface <code>SQLData</code>,
	 * the JDBC driver should call the method <code>SQLData.writeSQL</code> to write
	 * it to the SQL data stream. If, on the other hand, the object is of a class
	 * implementing <code>Ref</code>, <code>Blob</code>, <code>Clob</code>,
	 * <code>NClob</code>, <code>Struct</code>, <code>java.net.URL</code>, or
	 * <code>Array</code>, the driver should pass it to the database as a value of
	 * the corresponding SQL type.
	 * <P>
	 * This method throws an exception if there is an ambiguity, for example, if the
	 * object is of a class implementing more than one of the interfaces named
	 * above.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the object containing the input parameter value
	 * @exception SQLException                    if a database access error occurs,
	 *                                            this method is called on a closed
	 *                                            <code>CallableStatement</code> or
	 *                                            if the given <code>Object</code>
	 *                                            parameter is ambiguous
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getObject
	 * @since 1.4
	 */
	@Override
	public void setObject(String parameterName, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given input stream, which will have the
	 * specified number of bytes. When a very large ASCII value is input to a
	 * <code>LONGVARCHAR</code> parameter, it may be more practical to send it via a
	 * <code>InputStream</code>. Data will be read from the stream as needed until
	 * end-of-file is reached. The JDBC driver will do any necessary conversion from
	 * ASCII to the database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the Java input stream that contains the ASCII parameter
	 *                      value
	 * @param length        the number of bytes in the stream
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.4
	 */
	@Override
	public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given input stream, which will have the
	 * specified number of bytes. When a very large binary value is input to a
	 * <code>LONGVARBINARY</code> parameter, it may be more practical to send it via
	 * a <code>InputStream</code> object. The data will be read from the stream as
	 * needed until end-of-file is reached.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the java input stream which contains the binary
	 *                      parameter value
	 * @param length        the number of bytes in the stream
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.4
	 */
	@Override
	public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>Reader</code> object, which
	 * is the given number of characters long. When a very large UNICODE value is
	 * input to a <code>LONGVARCHAR</code> parameter, it may be more practical to
	 * send it via a <code>Reader</code> object. The data will be read from the
	 * stream as needed until end-of-file is reached. The JDBC driver will do any
	 * necessary conversion from UNICODE to the database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 *
	 * @param parameterName the name of the parameter
	 * @param reader        the <code>Reader</code> object that contains the UNICODE
	 *                      data used as the designated parameter
	 * @param length        the number of characters in the stream
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.4
	 */
	@Override
	public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given input stream. When a very large
	 * ASCII value is input to a <code>LONGVARCHAR</code> parameter, it may be more
	 * practical to send it via a <code>InputStream</code>. Data will be read from
	 * the stream as needed until end-of-file is reached. The JDBC driver will do
	 * any necessary conversion from ASCII to the database char format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>setAsciiStream</code> which takes
	 * a length parameter.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the Java input stream that contains the ASCII parameter
	 *                      value
	 * @exception SQLException if a database access error occurs or this method is
	 *                         called on a closed <code>CallableStatement</code>
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 * @since 1.6
	 */
	@Override
	public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given input stream. When a very large
	 * binary value is input to a <code>LONGVARBINARY</code> parameter, it may be
	 * more practical to send it via a <code>InputStream</code> object. The data
	 * will be read from the stream as needed until end-of-file is reached.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>setBinaryStream</code> which
	 * takes a length parameter.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the java input stream which contains the binary
	 *                      parameter value
	 * @exception SQLException if a database access error occurs or this method is
	 *                         called on a closed <code>CallableStatement</code>
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 * @since 1.6
	 */
	@Override
	public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>Reader</code> object. When a
	 * very large UNICODE value is input to a <code>LONGVARCHAR</code> parameter, it
	 * may be more practical to send it via a <code>Reader</code> object. The data
	 * will be read from the stream as needed until end-of-file is reached. The JDBC
	 * driver will do any necessary conversion from UNICODE to the database char
	 * format.
	 *
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream object
	 * or your own subclass that implements the standard interface.
	 * <P>
	 * <B>Note:</B> Consult your JDBC driver documentation to determine if it might
	 * be more efficient to use a version of <code>setCharacterStream</code> which
	 * takes a length parameter.
	 *
	 * @param parameterName the name of the parameter
	 * @param reader        the <code>Reader</code> object that contains the Unicode
	 *                      data
	 * @exception SQLException if a database access error occurs or this method is
	 *                         called on a closed <code>CallableStatement</code>
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
	 *                                         this method
	 * @since 1.6
	 */
	@Override
	public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>java.math.BigDecimal</code>
	 * value. The driver converts this to an SQL <code>NUMERIC</code> value when it
	 * sends it to the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getBigDecimal
	 * @since 1.4
	 */
	@Override
	public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given Java <code>String</code> value.
	 * The driver converts this to an SQL <code>VARCHAR</code> or
	 * <code>LONGVARCHAR</code> value (depending on the argument's size relative to
	 * the driver's limits on <code>VARCHAR</code> values) when it sends it to the
	 * database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getString
	 * @since 1.4
	 */
	@Override
	public void setString(String parameterName, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given Java array of bytes. The driver
	 * converts this to an SQL <code>VARBINARY</code> or <code>LONGVARBINARY</code>
	 * (depending on the argument's size relative to the driver's limits on
	 * <code>VARBINARY</code> values) when it sends it to the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getBytes
	 * @since 1.4
	 */
	@Override
	public void setBytes(String parameterName, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given <code>java.sql.Timestamp</code>
	 * value. The driver converts this to an SQL <code>TIMESTAMP</code> value when
	 * it sends it to the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getTimestamp
	 * @since 1.4
	 */
	@Override
	public void setTimestamp(String parameterName, java.sql.Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to SQL <code>NULL</code>.
	 *
	 * <P>
	 * <B>Note:</B> You must specify the parameter's SQL type.
	 *
	 * @param parameterName the name of the parameter
	 * @param sqlType       the SQL type code defined in <code>java.sql.Types</code>
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.4
	 */
	@Override
	public void setNull(String parameterName, int sqlType) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to SQL <code>NULL</code>. This version of the
	 * method <code>setNull</code> should be used for user-defined types and REF
	 * type parameters. Examples of user-defined types include: STRUCT, DISTINCT,
	 * JAVA_OBJECT, and named array types.
	 *
	 * <P>
	 * <B>Note:</B> To be portable, applications must give the SQL type code and the
	 * fully-qualified SQL type name when specifying a NULL user-defined or REF
	 * parameter. In the case of a user-defined type the name is the type name of
	 * the parameter itself. For a REF parameter, the name is the type name of the
	 * referenced type. If a JDBC driver does not need the type code or type name
	 * information, it may ignore it.
	 *
	 * Although it is intended for user-defined and Ref parameters, this method may
	 * be used to set a null parameter of any JDBC type. If the parameter does not
	 * have a user-defined or REF type, the given typeName is ignored.
	 *
	 *
	 * @param parameterName the name of the parameter
	 * @param sqlType       a value from <code>java.sql.Types</code>
	 * @param typeName      the fully-qualified name of an SQL user-defined type;
	 *                      ignored if the parameter is not a user-defined type or
	 *                      SQL <code>REF</code> value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.4
	 */
	@Override
	public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given Java <code>boolean</code> value.
	 * The driver converts this to an SQL <code>BIT</code> or <code>BOOLEAN</code>
	 * value when it sends it to the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException if a database access error occurs or this method is
	 *                         called on a closed <code>CallableStatement</code>
	 * @see #getBoolean
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @since 1.4
	 */
	@Override
	public void setBoolean(String parameterName, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given Java <code>byte</code> value. The
	 * driver converts this to an SQL <code>TINYINT</code> value when it sends it to
	 * the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getByte
	 * @since 1.4
	 */
	@Override
	public void setByte(String parameterName, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given Java <code>short</code> value. The
	 * driver converts this to an SQL <code>SMALLINT</code> value when it sends it
	 * to the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getShort
	 * @since 1.4
	 */
	@Override
	public void setShort(String parameterName, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given Java <code>int</code> value. The
	 * driver converts this to an SQL <code>INTEGER</code> value when it sends it to
	 * the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getInt
	 * @since 1.4
	 */
	@Override
	public void setInt(String parameterName, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given Java <code>long</code> value. The
	 * driver converts this to an SQL <code>BIGINT</code> value when it sends it to
	 * the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getLong
	 * @since 1.4
	 */
	@Override
	public void setLong(String parameterName, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given Java <code>float</code> value. The
	 * driver converts this to an SQL <code>FLOAT</code> value when it sends it to
	 * the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getFloat
	 * @since 1.4
	 */
	@Override
	public void setFloat(String parameterName, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * Sets the designated parameter to the given Java <code>double</code> value.
	 * The driver converts this to an SQL <code>DOUBLE</code> value when it sends it
	 * to the database.
	 *
	 * @param parameterName the name of the parameter
	 * @param x             the parameter value
	 * @exception SQLException                    if a database access error occurs
	 *                                            or this method is called on a
	 *                                            closed
	 *                                            <code>CallableStatement</code>
	 * @exception SQLFeatureNotSupportedException if the JDBC driver does not
	 *                                            support this method
	 * @see #getDouble
	 * @since 1.4
	 */
	@Override
	public void setDouble(String parameterName, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException(MESSAGE_FEATURE_NOT_SUPPORTED);
	}

	/**
	 * This method re populates the resBundle during the deserialization process
	 *
	 */
	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		// Default state initialization happens here
		ois.defaultReadObject();
		// Initialization of transient Res Bundle happens here .
	}

	// ------------------------- JDBC 4.1 -----------------------------------
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		throw new SQLFeatureNotSupportedException("Not supported yet.");
	}

	static final long serialVersionUID = 1884577171200622428L;
}
