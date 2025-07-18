/*-------------------------------------------------------------------------
 *
 * HiveJdbcClient.java
 * 		The java class containing all the functions to query hive db
 *
 * Copyright (c) 2019-2025, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		HiveJdbcClient.java
 *
 *-------------------------------------------------------------------------
 */

/*
 * To compile issue
 * 
 * javac MsgBuf.java
 * javac HiveJdbcClient.java
 * 
 * rm HiveJdbcClient-1.0.jar 
 * jar cf HiveJdbcClient-1.0.jar *.class
 * cp HiveJdbcClient-1.0.jar /usr/local/edb95/lib/postgresql/
 * 
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.Date;

public class HiveJdbcClient
{
	private static String		m_driverName = "org.apache.hive.jdbc.HiveDriver";

	private static final int	m_authTypeUnspecified = 0;
	private static final int	m_authTypeNoSasl = 1;
	private static final int	m_authTypeLDAP = 2;

	private static final int	m_clientTypeHive = 0;
	private static final int	m_clientTypeSpark = 1;

	private int					m_queryTimeout = 0;
	private boolean				m_isDebug = false;
	private int					m_nestingLimit = 100;
	private boolean 			m_isInitialized = false;

	private Connection[]		m_hdfsConnection;
	private PreparedStatement[]	m_preparedStatement;
	private ResultSet[]			m_resultSet;
	private ResultSetMetaData[]	m_resultSetMetaData;
	private boolean[]			m_isFree;
	private MsgBuf[]			m_host;
	private int[]				m_port;
	private MsgBuf[]			m_user;
	private int[]				m_fetchCount;
	private int[]				m_tempCount;

	public int FindFreeSlot()
	{
		for (int i = 0; i < m_nestingLimit; i++)
		{
			if (m_isFree[i])
			{
				if (m_isDebug)
					System.out.println("Index " + i + " is free");

				m_isFree[i] = false;
				return (i);
			}
		}
		return (-1);
	}

	/* singature will be (Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;IIILMsgBuf;)I */
	public int DBOpenConnection(String host, int port, String databaseName,
								String userName, String password,
								int connectTimeout, int receiveTimeout,
								int authType, int clientType, MsgBuf errBuf)
	{
		int index;
		String hst;
		String usr;

		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBOpenConnection");

		if (!m_isInitialized)
		{
			m_hdfsConnection = new Connection[m_nestingLimit];
			m_preparedStatement = new PreparedStatement[m_nestingLimit];
			m_resultSet = new ResultSet[m_nestingLimit];
			m_resultSetMetaData = new ResultSetMetaData[m_nestingLimit];
			m_isFree = new boolean[m_nestingLimit];
			m_host = new MsgBuf[m_nestingLimit];
			m_port = new int[m_nestingLimit];
			m_user = new MsgBuf[m_nestingLimit];

			m_fetchCount = new int[m_nestingLimit];
			m_tempCount = new int[m_nestingLimit];

			for (int i = 0; i < m_nestingLimit; i++)
			{
				m_hdfsConnection[i] = null;
				m_preparedStatement[i] = null;
				m_resultSet[i] = null;
				m_resultSetMetaData[i] = null;
				m_isFree[i] = true;
				m_host[i] = new MsgBuf("xxx.xxx.xxx.xxx or localhost");
				m_port[i] = 0;
				m_fetchCount[i] = 0;
				m_tempCount[i] = 0;

				m_user[i] = new MsgBuf("store user name here");
			}
			m_isInitialized = true;
		}

		index = FindFreeSlot();
		if (index < 0)
		{
			errBuf.catVal("ERROR : Internal error, no free slot");
			return (-1);
		}

		m_queryTimeout = receiveTimeout;

		if (m_hdfsConnection[index] != null)
		{
			hst = m_host[index].getVal();
			usr = m_user[index].getVal();

			if (m_port[index] == port &&
				hst.equalsIgnoreCase(host) &&
				usr.equalsIgnoreCase(userName))
			{
				errBuf.catVal("Reusing connection ["+ index + "] ");
				return (index);
			}
			else
			{
				/*
				 * Release existing connection and
				 * reconnect with new credentials
				 */
				DBCloseConnection(index);
				m_isFree[index] = false;
			}
		}

		try
		{
			Class.forName(m_driverName);
		}
		catch (ClassNotFoundException e)
		{
			errBuf.catVal("ERROR : Hive JDBC Driver was not found, ");
			errBuf.catVal("make sure hive-jdbc-x.x.x-standalone.jar is in class path");
			m_isFree[index] = true;
			return (-2);
		}

		DriverManager.setLoginTimeout(connectTimeout);

		String conURL = "jdbc:hive2://";
		conURL += host;
		conURL += ":";
		conURL += port;
		conURL += "/";
		conURL += databaseName;

		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBOpenConnection attempting to connect to " + conURL);

		try
		{
			switch (authType)
			{
				case m_authTypeUnspecified:
					if (userName == null || userName.equals(""))
					{
						conURL += ";auth=noSasl";
						m_hdfsConnection[index] = DriverManager.getConnection(conURL, "userName", "password");
					}
					else
					{
						m_hdfsConnection[index] = DriverManager.getConnection(conURL, userName, password);
					}
					break;

				case m_authTypeNoSasl:
					conURL += ";auth=noSasl";
					if (userName == null || userName.equals(""))
					{
						errBuf.catVal("ERROR : A valid user name is required");
						m_isFree[index] = true;
						return (-3);
					}
					else
					{
						m_hdfsConnection[index] = DriverManager.getConnection(conURL, userName, password);
					}
					break;

				case m_authTypeLDAP:
					if (userName == null || userName.equals(""))
					{
						errBuf.catVal("ERROR : A valid user name is required");
						m_isFree[index] = true;
						return (-4);
					}
					else
					{
						m_hdfsConnection[index] = DriverManager.getConnection(conURL, userName, password);
					}
					break;

				default:
					if (userName == null || userName.equals(""))
					{
						conURL += ";auth=noSasl";
						m_hdfsConnection[index] = DriverManager.getConnection(conURL, "userName", "password");
					}
					else
					{
						m_hdfsConnection[index] = DriverManager.getConnection(conURL, userName, password);
					}
					break;
			}
		}
		catch (SQLException ex)
		{
			if (m_isDebug)
				System.out.println("HiveJdbcClient::DBOpenConnection could not connect to " + conURL);

			errBuf.catVal("ERROR : Could not connect to ");
			errBuf.catVal(conURL);
			errBuf.catVal(" within ");
			errBuf.catVal(DriverManager.getLoginTimeout());
			errBuf.catVal(" seconds");
			m_isFree[index] = true;
			return (-5);
		}

		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBOpenConnection connected to " + conURL);

		m_host[index].resetVal();
		m_host[index].catVal(host);

		m_user[index].resetVal();
		if (userName == null)
			m_user[index].catVal("");
		else
			m_user[index].catVal(userName);

		m_port[index] = port;

		errBuf.catVal("Connected ["+ index + "] to ");
		errBuf.catVal(conURL);

		return (index);
	}

	/* singature will be (I)I */
	public int DBCloseConnection(int index)
	{
		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBCloseConnection");

		if (m_resultSet[index] != null)
		{
			try
			{
				m_resultSet[index].close();
				m_resultSet[index] = null;
			}
			catch (SQLException e)
			{
				/* ignored */
			}
		}

		if (m_preparedStatement[index] != null)
		{
			try
			{
				m_preparedStatement[index].close();
				m_preparedStatement[index] = null;
			}
			catch (SQLException e1)
			{
				/* ignored */
			}
		}

		m_isFree[index] = true;

		if (m_hdfsConnection[index] != null)
		{
			try
			{
				m_hdfsConnection[index].close();
				m_hdfsConnection[index] = null;
			}
			catch (SQLException e)
			{
				/* ignored */
				return (-1);
			}
		}
		return (0);
	}

	/* singature will be ()I */
	public int DBCloseAllConnections()
	{
		int count = 0;

		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBCloseAllConnections");

		for (int i = 0; i < m_nestingLimit; i++)
		{
			if (m_isFree[i] == false)
			{
				count++;
				DBCloseConnection(i);
			}
		}
		return (count);
	}

	/* singature will be (ILjava/lang/String;ILMsgBuf;)I */
	public int DBPrepare(int index, String query, int maxRows, MsgBuf errBuf)
	{
		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBPrepare");

		if (m_hdfsConnection[index] == null)
		{
			errBuf.catVal("Database is not connected");
			m_isFree[index] = true;
			return (-1);
		}

		if (m_resultSet[index] != null)
		{
			try
			{
				m_resultSet[index].close();
				m_resultSet[index] = null;
			}
			catch (SQLException e)
			{
				m_isFree[index] = true;
				errBuf.catVal(e.getMessage());
				return (-2);
			}
		}

		if (m_preparedStatement[index] != null)
		{
			try
			{
				m_preparedStatement[index].close();
				m_preparedStatement[index] = null;
			}
			catch (SQLException e)
			{
				m_isFree[index] = true;
				errBuf.catVal(e.getMessage());
				return (-3);
			}
		}

		try
		{
			m_preparedStatement[index] = m_hdfsConnection[index].prepareStatement(query);
			m_preparedStatement[index].setFetchSize(maxRows);
			m_fetchCount[index] = 0;
			m_tempCount[index] = 0;
			/* TODO This method is not supported */
//			m_preparedStatement[index].setQueryTimeout(m_queryTimeout);
		}
		catch (SQLException e)
		{
			m_isFree[index] = true;

			errBuf.catVal(e.getMessage());

			if (m_preparedStatement[index] != null)
			{
				try
				{
					m_preparedStatement[index].close();
					m_preparedStatement[index] = null;
				}
				catch (SQLException e1)
				{
					/* ignored */
				}
			}
			return (-4);
		}
		return (0);
	}

	/* singature will be (ILJDBCType;LMsgBuf;)I */
	public int DBBindVar(int index, int paramIndex, JDBCType paramToBind, MsgBuf errBuf)
	{
		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBBind");

		if (m_hdfsConnection[index] == null)
		{
			errBuf.catVal("Database is not connected");
			m_isFree[index] = true;
			return (-1);
		}

		if (m_preparedStatement[index] == null)
		{
			errBuf.catVal("Statement is not prepared");
			m_isFree[index] = true;
			return (-2);
		}

		if (paramToBind.getType() < 0)
		{
			errBuf.catVal("Invalid parameter type");

			if (m_preparedStatement[index] != null)
			{
				try
				{
					m_preparedStatement[index].close();
					m_preparedStatement[index] = null;
				}
				catch (SQLException e1)
				{
					/* ignored */
				}
			}

			m_isFree[index] = true;
			return (-3);
		}

		if (m_resultSet[index] != null)
		{
			try
			{
				m_resultSet[index].close();
				m_resultSet[index] = null;
			}
			catch (SQLException e)
			{
				if (m_preparedStatement[index] != null)
				{
					try
					{
						m_preparedStatement[index].close();
						m_preparedStatement[index] = null;
					}
					catch (SQLException e1)
					{
						/* ignored */
					}
				}

				m_isFree[index] = true;
				errBuf.catVal(e.getMessage());
				return (-4);
			}
		}
		try
		{
			switch (paramToBind.getType())
			{
				case 1:
					m_preparedStatement[index].setBoolean(paramIndex, paramToBind.getBool());
					break;
				case 2:
					m_preparedStatement[index].setShort(paramIndex, paramToBind.getShort());
					break;
				case 3:
					m_preparedStatement[index].setInt(paramIndex, paramToBind.getInt());
					break;
				case 4:
					m_preparedStatement[index].setLong(paramIndex, paramToBind.getLong());
					break;
				case 5:
					m_preparedStatement[index].setDouble(paramIndex, paramToBind.getDoub());
					break;
				case 6:
					m_preparedStatement[index].setFloat(paramIndex, paramToBind.getFloat());
					break;
				case 7:
					m_preparedStatement[index].setString(paramIndex, paramToBind.getString());
					break;
				case 8:
					m_preparedStatement[index].setDate(1, paramToBind.getDate());
					break;
				case 9:
					m_preparedStatement[index].setTime(paramIndex, paramToBind.getTime());
					break;
				case 10:
					m_preparedStatement[index].setTimestamp(paramIndex, paramToBind.getStamp());
					break;
			}
		}
		catch (SQLException e)
		{
			m_isFree[index] = true;

			errBuf.catVal(e.getMessage());

			if (m_preparedStatement[index] != null)
			{
				try
				{
					m_preparedStatement[index].close();
					m_preparedStatement[index] = null;
				}
				catch (SQLException e1)
				{
					/* ignored */
				}
			}
			return (-5);
		}
		return (0);
	}

	/* singature will be (ILMsgBuf;)I */
	public int DBExecutePrepared(int index, MsgBuf errBuf)
	{
		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBExecutePrepared");

		if (m_hdfsConnection[index] == null)
		{
			errBuf.catVal("Database is not connected");
			m_isFree[index] = true;
			return (-1);
		}

		if (m_preparedStatement[index] == null)
		{
			errBuf.catVal("Statement is not prepared");
			m_isFree[index] = true;
			return (-2);
		}

		if (m_resultSet[index] != null)
		{
			try
			{
				m_resultSet[index].close();
				m_resultSet[index] = null;
			}
			catch (SQLException e)
			{
				if (m_preparedStatement[index] != null)
				{
					try
					{
						m_preparedStatement[index].close();
						m_preparedStatement[index] = null;
					}
					catch (SQLException e1)
					{
						/* ignored */
					}
				}

				m_isFree[index] = true;
				errBuf.catVal(e.getMessage());
				return (-3);
			}
		}

		try
		{
			m_resultSet[index] = m_preparedStatement[index].executeQuery();
			m_resultSetMetaData[index] = m_resultSet[index].getMetaData();
		}
		catch (SQLException e)
		{
			m_isFree[index] = true;

			errBuf.catVal(e.getMessage());

			if (m_resultSet[index] != null)
			{
				try
				{
					m_resultSet[index].close();
					m_resultSet[index] = null;
				}
				catch (SQLException e1)
				{
					/* ignored */
				}
			}

			if (m_preparedStatement[index] != null)
			{
				try
				{
					m_preparedStatement[index].close();
					m_preparedStatement[index] = null;
				}
				catch (SQLException e1)
				{
					/* ignored */
				}
			}
			return (-5);
		}

		return (0);
	}

	/* singature will be (ILjava/lang/String;ILMsgBuf;)I */
	public int DBExecute(int index, String query, int maxRows, MsgBuf errBuf)
	{
		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBExecute");

		if (m_hdfsConnection[index] == null)
		{
			errBuf.catVal("Database is not connected");
			m_isFree[index] = true;
			return (-1);
		}

		if (m_resultSet[index] != null)
		{
			try
			{
				m_resultSet[index].close();
				m_resultSet[index] = null;
			}
			catch (SQLException e)
			{
				m_isFree[index] = true;
				errBuf.catVal(e.getMessage());
				return (-2);
			}
		}

		if (m_preparedStatement[index] != null)
		{
			try
			{
				m_preparedStatement[index].close();
				m_preparedStatement[index] = null;
			}
			catch (SQLException e)
			{
				m_isFree[index] = true;
				errBuf.catVal(e.getMessage());
				return (-3);
			}
		}

		try
		{
			m_preparedStatement[index] = m_hdfsConnection[index].prepareStatement(query);
			m_preparedStatement[index].setFetchSize(maxRows);
			/* TODO This method is not supported */
//			m_preparedStatement[index].setQueryTimeout(m_queryTimeout);
		}
		catch (SQLException e)
		{
			m_isFree[index] = true;

			errBuf.catVal(e.getMessage());

			if (m_preparedStatement[index] != null)
			{
				try
				{
					m_preparedStatement[index].close();
					m_preparedStatement[index] = null;
				}
				catch (SQLException e1)
				{
					/* ignored */
				}
			}
			return (-4);
		}

		try
		{
			m_resultSet[index] = m_preparedStatement[index].executeQuery();
			m_resultSetMetaData[index] = m_resultSet[index].getMetaData();
		}
		catch (SQLException e)
		{
			m_isFree[index] = true;

			errBuf.catVal(e.getMessage());

			if (m_resultSet[index] != null)
			{
				try
				{
					m_resultSet[index].close();
					m_resultSet[index] = null;
				}
				catch (SQLException e1)
				{
					/* ignored */
				}
			}

			if (m_preparedStatement[index] != null)
			{
				try
				{
					m_preparedStatement[index].close();
					m_preparedStatement[index] = null;
				}
				catch (SQLException e1)
				{
					/* ignored */
				}
			}
			return (-5);
		}
		return (0);
	}


	/* singature will be (ILjava/lang/String;LMsgBuf;)I */
	public int DBExecuteUtility(int index, String query, MsgBuf errBuf)
	{
		boolean bret;

		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBExecute");

		if (m_hdfsConnection[index] == null)
		{
			m_isFree[index] = true;
			errBuf.catVal("Database is not connected");
			return (-1);
		}

		if (m_preparedStatement[index] != null)
		{
			try
			{
				m_preparedStatement[index].close();
				m_preparedStatement[index] = null;
			}
			catch (SQLException e)
			{
				m_isFree[index] = true;
				errBuf.catVal(e.getMessage());
				return (-3);
			}
		}

		try
		{
			m_preparedStatement[index] = m_hdfsConnection[index].prepareStatement(query);
			/* TODO This method is not supported */
//			m_preparedStatement[index].setQueryTimeout(m_queryTimeout);
		}
		catch (SQLException e)
		{
			m_isFree[index] = true;

			errBuf.catVal(e.getMessage());

			if (m_preparedStatement[index] != null)
			{
				try
				{
					m_preparedStatement[index].close();
					m_preparedStatement[index] = null;
				}
				catch (SQLException e1)
				{
					/* ignored */
				}
			}
			return (-4);
		}

		try
		{
			bret = m_preparedStatement[index].execute();
		}
		catch (SQLException e)
		{
			m_isFree[index] = true;

			errBuf.catVal(e.getMessage());

			if (m_preparedStatement[index] != null)
			{
				try
				{
					m_preparedStatement[index].close();
					m_preparedStatement[index] = null;
				}
				catch (SQLException e1)
				{
					/* ignored */
				}
			}
			return (-5);
		}

		if (!bret)
		{
			/* query did not generate any result set, and was not supposed to do either */
			return (0);
		}
		m_isFree[index] = true;
		errBuf.catVal("This function is supposed to execute queries that do not generate any result set");
		return (-6);
	}

	/* singature will be (ILMsgBuf;)I */
	public int DBCloseResultSet(int index, MsgBuf errBuf)
	{
		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBCloseResultSet");

		if (m_resultSet[index] != null)
		{
			try
			{
				m_resultSet[index].close();
				m_resultSet[index] = null;
			}
			catch (SQLException e)
			{
				errBuf.catVal(e.getMessage());
				return (-1);
			}
		}
		return (0);
	}

	/* singature will be (ILMsgBuf;)I */
	public int DBFetch(int index, MsgBuf errBuf)
	{
		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBFetch");

		if (m_resultSet[index] == null)
		{
			m_isFree[index] = true;
			errBuf.catVal("Resultset is null");
			return (-2);
		}

//		if (m_isDebug)
		{
			m_tempCount[index]++;
			m_fetchCount[index]++;

			if (m_tempCount[index] > 100000)
			{
				m_tempCount[index] = 0;
				System.out.println("HiveJdbcClient::DBFetch [" + m_fetchCount[index] + "]");
			}
		}

		/* The hive JDBC driver does not support isClosed or isAfterLast methods */
		try
		{
			if (!m_resultSet[index].next())
			{
				errBuf.catVal("All rows have already been fetched");
				return (-1);
			}
		}
		catch (SQLException e)
		{
			m_isFree[index] = true;
			errBuf.catVal(e.getMessage());
			return (-3);
		}

		return (0);
	}

	/* singature will be (ILMsgBuf;)I */
	public int DBGetColumnCount(int index, MsgBuf errBuf)
	{
		int colCount;

		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBGetColumnCount");

		if (m_resultSetMetaData[index] == null)
		{
			m_isFree[index] = true;
			errBuf.catVal("Resultset meta data is null");
			return (-1);
		}

		try
		{
			colCount = m_resultSetMetaData[index].getColumnCount();
		}
		catch (SQLException e)
		{
			m_isFree[index] = true;
			errBuf.catVal(e.getMessage());
			return (-2);
		}
		return (colCount);
	}

	/* singature will be (IILMsgBuf;LMsgBuf;)I */
	public int DBGetFieldAsCString(int index, int columnIdx, MsgBuf dataBuf, MsgBuf errBuf)
	{
		String val;

		if (m_isDebug)
			System.out.println("HiveJdbcClient::DBGetFieldAsCString");

		if (m_resultSet[index] == null)
		{
			m_isFree[index] = true;
			errBuf.catVal("Resultset is null");
			return (-2);
		}

		try
		{
			val = m_resultSet[index].getString(columnIdx + 1);
		}
		catch (SQLException e)
		{
			m_isFree[index] = true;
			errBuf.catVal(e.getMessage());
			return (-5);
		}

		/* This is the only way to let the caller know value is null */
		if (val == null)
			return (-1);

		dataBuf.catVal(val);

		return (val.length());
	}
}
