package dax;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

public class AbstractConnection implements Connection {
	
	private Connection _connection;

	public AbstractConnection(Callable<Connection> connectionFunction) throws SQLException, ClassNotFoundException {
		try {
			_connection = connectionFunction.call();
		} catch (Exception e) {
			if (e instanceof SQLException) {
				throw (SQLException)e;
			} else if (e instanceof ClassNotFoundException) {
				throw (ClassNotFoundException)e;
			} else {
				throw new ClassNotFoundException(e.getMessage());
			}
		}
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		return _connection.isWrapperFor(arg0);
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		
		return _connection.unwrap(arg0);
	}

	@Override
	public void abort(Executor arg0) throws SQLException {
		_connection.abort(arg0);
	}

	@Override
	public void clearWarnings() throws SQLException {
		_connection.clearWarnings();
	}

	@Override
	public void close() throws SQLException {
		_connection.close();
	}

	@Override
	public void commit() throws SQLException {
		_connection.commit();
	}

	@Override
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
		
		return _connection.createArrayOf(arg0, arg1);
	}

	@Override
	public Blob createBlob() throws SQLException {

		return _connection.createBlob();
	}

	@Override
	public Clob createClob() throws SQLException {

		return _connection.createClob();
	}

	@Override
	public NClob createNClob() throws SQLException {
return _connection.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		
		return _connection.createSQLXML();
	}

	@Override
	public Statement createStatement() throws SQLException {
		
		return _connection.createStatement();
	}

	@Override
	public Statement createStatement(int arg0, int arg1) throws SQLException {
		
		return _connection.createStatement(arg0, arg1);
	}

	@Override
	public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
		
		return _connection.createStatement(arg0, arg2);
	}

	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
		
		return _connection.createStruct(arg0, arg1);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		
		return _connection.getAutoCommit();
	}

	@Override
	public String getCatalog() throws SQLException {
		
		return _connection.getCatalog();
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		
		return _connection.getClientInfo();
	}

	@Override
	public String getClientInfo(String arg0) throws SQLException {
		
		return _connection.getClientInfo(arg0);
	}

	@Override
	public int getHoldability() throws SQLException {
		
		return _connection.getHoldability();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		
		return _connection.getMetaData();
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		
		return _connection.getNetworkTimeout();
	}

	@Override
	public String getSchema() throws SQLException {
		
		return _connection.getSchema();
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		
		return _connection.getTransactionIsolation();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		
		return _connection.getTypeMap();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		
		return _connection.getWarnings();
	}

	@Override
	public boolean isClosed() throws SQLException {
		
		return _connection.isClosed();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		
		return _connection.isClosed();
	}

	@Override
	public boolean isValid(int arg0) throws SQLException {
		
		return _connection.isValid(arg0);
	}

	@Override
	public String nativeSQL(String arg0) throws SQLException {
		
		return _connection.nativeSQL(arg0);
	}

	@Override
	public CallableStatement prepareCall(String arg0) throws SQLException {
		
		return _connection.prepareCall(arg0);
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException {
		
		return _connection.prepareCall(arg0, arg1, arg2);
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException {
		
		return _connection.prepareCall(arg0, arg1, arg2);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0) throws SQLException {
		
		return _connection.prepareStatement(arg0);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException {
		
		return _connection.prepareStatement(arg0, arg1);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException {
		
		return _connection.prepareStatement(arg0, arg1);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException {
		
		return _connection.prepareStatement(arg0, arg1);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException {
		
		return _connection.prepareStatement(arg0, arg1);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException {
		
		return _connection.prepareStatement(arg0, arg1);
	}

	@Override
	public void releaseSavepoint(Savepoint arg0) throws SQLException {
		_connection.releaseSavepoint(arg0);
	}

	@Override
	public void rollback() throws SQLException {
		_connection.rollback();

	}

	@Override
	public void rollback(Savepoint arg0) throws SQLException {
		_connection.rollback(arg0);

	}

	@Override
	public void setAutoCommit(boolean arg0) throws SQLException {
		
		_connection.setAutoCommit(arg0);
	}

	@Override
	public void setCatalog(String arg0) throws SQLException {
		
		_connection.setCatalog(arg0);
	}

	@Override
	public void setClientInfo(Properties arg0) throws SQLClientInfoException {
		
		_connection.setClientInfo(arg0);
	}

	@Override
	public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException {
		_connection.setClientInfo(arg0, arg1);

	}

	@Override
	public void setHoldability(int arg0) throws SQLException {
		_connection.setHoldability(arg0);

	}

	@Override
	public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
		_connection.setNetworkTimeout(arg0, arg1);

	}

	@Override
	public void setReadOnly(boolean arg0) throws SQLException {
		_connection.setReadOnly(arg0);

	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		
		return _connection.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String arg0) throws SQLException {
		
		return _connection.setSavepoint(arg0);
	}

	@Override
	public void setSchema(String arg0) throws SQLException {
		_connection.setSchema(arg0);
	}

	@Override
	public void setTransactionIsolation(int arg0) throws SQLException {
		_connection.setTransactionIsolation(arg0);

	}

	@Override
	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
		_connection.setTypeMap(arg0);
	}

}
