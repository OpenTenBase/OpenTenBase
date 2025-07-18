/*-------------------------------------------------------------------------
 *
 * JDBCType.java
 * 		Class to store parameter values
 *
 * Copyright (c) 2019-2025, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		JDBCType.java
 *
 *-------------------------------------------------------------------------
 */

import java.lang.String;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class JDBCType
{
	private static final int	m_noType = 0;

	private static final int	m_boolType = 1;
	private static final int	m_shortType = 2;
	private static final int	m_intType = 3;
	private static final int	m_longType = 4;

	private static final int	m_doubType = 5;
	private static final int	m_floatType = 6;

	private static final int	m_stringType = 7;
	private static final int	m_dateType = 8;
	private static final int	m_timeType = 9;
	private static final int	m_stampType = 10;

	private int			m_jdbcType;

	private boolean		m_boolVal;
	private short		m_shortVal;
	private int			m_intVal;
	private long		m_longVal;

	private double		m_doubleVal;
	private float		m_floatVal;

	private String		m_stringVal;
	private Date		m_dateVal;
	private Time		m_timeVal;
	private Timestamp	m_stampVal;

	public JDBCType(int typ)
	{
		m_jdbcType = typ;
	}

	public int getType()
	{
		if (m_jdbcType < m_boolType || m_jdbcType > m_stampType)
			return -1;
		return m_jdbcType;
	}

	public void setBool(boolean x)
	{
		m_boolVal = x;
		m_jdbcType = m_boolType;
	}

	public void setShort(short x)
	{
		m_shortVal = x;
		m_jdbcType = m_shortType;
	}

	public void setInt(int x)
	{
		m_intVal = x;
		m_jdbcType = m_intType;
	}

	public void setLong(long x)
	{
		m_longVal = x;
		m_jdbcType = m_longType;
	}

	public void setDoub(double x)
	{
		m_doubleVal = x;
		m_jdbcType = m_doubType;
	}

	public void setFloat(float x)
	{
		m_floatVal = x;
		m_jdbcType = m_floatType;
	}

	public void setString(String x)
	{
		m_stringVal = x;
		m_jdbcType = m_stringType;
	}

	public void setDate(String s)
	{
		Date x = Date.valueOf(s);
		m_dateVal = x;
		m_jdbcType = m_dateType;
	}

	public void setTime(String s)
	{
		Time x = Time.valueOf(s);
		m_timeVal = x;
		m_jdbcType = m_timeType;
	}

	public void setStamp(String s)
	{
		Timestamp x = Timestamp.valueOf(s);
		m_stampVal = x;
		m_jdbcType = m_stampType;
	}


	public boolean getBool()
	{
		return m_boolVal;
	}

	public short getShort()
	{
		return m_shortVal;
	}

	public int getInt()
	{
		return m_intVal;
	}

	public long getLong()
	{
		return m_longVal;
	}

	public double getDoub()
	{
		return m_doubleVal;
	}

	public float getFloat()
	{
		return m_floatVal;
	}

	public String getString()
	{
		return m_stringVal;
	}

	public Date getDate()
	{
		return m_dateVal;
	}

	public Time getTime()
	{
		return m_timeVal;
	}

	public Timestamp getStamp()
	{
		return m_stampVal;
	}
}
