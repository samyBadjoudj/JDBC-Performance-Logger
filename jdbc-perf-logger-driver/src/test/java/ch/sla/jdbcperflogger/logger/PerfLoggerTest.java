/*
 *  Copyright 2013 Sylvain LAURENT
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.sla.jdbcperflogger.logger;

import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import ch.sla.jdbcperflogger.DatabaseType;
import ch.sla.jdbcperflogger.model.PreparedStatementValuesHolder;
import ch.sla.jdbcperflogger.model.SqlTypedValue;

public class PerfLoggerTest {


    @Test
    public void testfillParameters() throws Exception {
        final PreparedStatementValuesHolder valHolder = new PreparedStatementValuesHolder();
        String filledSql;

        valHolder.put(1, new SqlTypedValue("toto$", Types.VARCHAR));
        filledSql = PerfLogger.fillParameters( "select * from toto where name = ?", valHolder, DatabaseType.ORACLE);
        Assert.assertEquals("select * from toto where name = 'toto$' /*VARCHAR*/", filledSql);

        valHolder.put(2, new SqlTypedValue(36, Types.INTEGER));
        filledSql = PerfLogger.fillParameters("select * from toto where name = ? and age < ?", valHolder,
                DatabaseType.ORACLE);
        Assert.assertEquals("select * from toto where name = 'toto$' /*VARCHAR*/ and age < 36 /*INTEGER*/", filledSql);

        valHolder.put(1, new SqlTypedValue(null, Types.VARCHAR));
        filledSql = PerfLogger.fillParameters("select * from toto where name = ?", valHolder, DatabaseType.ORACLE);
        Assert.assertEquals("select * from toto where name = NULL /*VARCHAR*/", filledSql);

        valHolder.put(1, new SqlTypedValue(null, Types.DATE));
        filledSql = PerfLogger.fillParameters( "select * from toto where param = ?", valHolder, DatabaseType.ORACLE);
        Assert.assertEquals("select * from toto where param = NULL /*DATE*/", filledSql);

        valHolder.put(1, new SqlTypedValue(java.sql.Date.valueOf("2013-03-04"), Types.DATE));
        filledSql = PerfLogger.fillParameters( "select * from toto where param = ?", valHolder, DatabaseType.ORACLE);
        Assert.assertEquals("select * from toto where param = date'2013-03-04' /*DATE*/", filledSql);

        final String dateStr = "2011-07-15T13:45:56.123";
        final Date utilDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(dateStr);
        valHolder.put(1, new SqlTypedValue(utilDate, Types.DATE));
        filledSql = PerfLogger.fillParameters( "select * from toto where param = ?", valHolder, DatabaseType.ORACLE);
        Assert.assertEquals(
                "select * from toto where param = cast(timestamp'2011-07-15 13:45:56.123' as DATE) /*DATE (non pure)*/",
                filledSql);

        final Timestamp tstamp = Timestamp.valueOf("2011-07-15 13:45:56.123");
        valHolder.put(1, new SqlTypedValue(tstamp, Types.TIMESTAMP));
        filledSql = PerfLogger.fillParameters( "select * from toto where param = ?", valHolder, DatabaseType.ORACLE);
        Assert.assertEquals("select * from toto where param = timestamp'" + tstamp + "' /*TIMESTAMP*/", filledSql);

        valHolder.put(1, new SqlTypedValue(new Object(), Types.CLOB));
        filledSql = PerfLogger.fillParameters( "select * from toto where param = ?", valHolder, DatabaseType.ORACLE);
        Assert.assertEquals("select * from toto where param = ? /*CLOB*/", filledSql);
    }

    @Test
    public void testgetValueAsString_string() {
        String val;

        val = PerfLogger.getValueAsString(new SqlTypedValue("toto", Types.VARCHAR), DatabaseType.ORACLE);
        Assert.assertEquals("'toto' /*VARCHAR*/", val);

        val = PerfLogger.getValueAsString(new SqlTypedValue("hel'lo", Types.VARCHAR), DatabaseType.ORACLE);
        Assert.assertEquals("'hel''lo' /*VARCHAR*/", val);

        val = PerfLogger.getValueAsString(new SqlTypedValue("java's cool, it''s", Types.VARCHAR), DatabaseType.ORACLE);
        Assert.assertEquals("'java''s cool, it''''s' /*VARCHAR*/", val);
    }

    @Test
    public void removeComments(){
        String sqlStatement ="insert into  /* removed one */emp (?,?)/*ending comment*/";
        Assert.assertEquals("insert into  emp (?,?)", PerfLogger.removeComments(sqlStatement));
    }
    @Test
    public void removeCommentsFailure(){
        String sqlStatement ="insert into  /* removed one */emp (?,?)/*ending comment";
        Assert.assertEquals(sqlStatement, PerfLogger.removeComments(sqlStatement));
    }
    @Test
    public void removeCommentsWithoutComments(){
        String sqlStatement ="insert into  emp (?,?)";
        Assert.assertEquals(sqlStatement, PerfLogger.removeComments(sqlStatement));
    }
    @Test
    public void removeCommentsWithoutCommentsWithString(){
        String sqlStatement ="insert into  emp (?,?,'test')";
        Assert.assertEquals(sqlStatement, PerfLogger.removeComments(sqlStatement));
    }    


    @Test
    public void removeCommentsWithoutCommentsWithStringAndComments(){
        String sqlStatement ="/* removed one */insert into  emp (?,?,'test')";
        Assert.assertEquals("insert into  emp (?,?,'test')", PerfLogger.removeComments(sqlStatement));
    }
    @Test
    public void removeCommentsWithoutEmpty(){
        String sqlStatement ="";
        Assert.assertEquals(sqlStatement, PerfLogger.removeComments(sqlStatement));
    }
    @Test
    public void removeCommentsWithString(){
        String sqlStatement ="insert into /*+ PARALLEL(4) */  /* removed one ? */emp (?,?,'/*test*/')/*ending comment*/";
        Assert.assertEquals("insert into /*+ PARALLEL(4) */  emp (?,?,'/*test*/')", PerfLogger.removeComments(sqlStatement));
    }

    @Test
    public void removeCommentsWithStringNoClosed(){
        String sqlStatement ="insert into  /* removed one ?*/emp (?,?,'/*test*/)/*ending comment*/";
        Assert.assertEquals(sqlStatement, PerfLogger.removeComments(sqlStatement));
    }
    @Test
    public void removeCommentsWitDBMSHints(){
        String sqlStatement ="SELECT /*+ FIRST_ROWS(10) */ * FROM employees;";
        Assert.assertEquals("SELECT /*+ FIRST_ROWS(10) */ * FROM employees;", PerfLogger.removeComments(sqlStatement));

        sqlStatement ="/*comment*/ SELECT /*+ PARALLEL(4) */ hr_emp.last_name, d.department_name " +
                "FROM   employees hr_emp, departments d" +
                "WHERE  hr_emp.department_id=d.department_id;";
        Assert.assertEquals(" SELECT /*+ PARALLEL(4) */ hr_emp.last_name, d.department_name FROM   employees hr_emp, departments dWHERE  hr_emp.department_id=d.department_id;", PerfLogger.removeComments(sqlStatement));


    }

    @Test
    public void removeCommentsWithoutCommentsWithStringAndEscapingString(){
        String sqlStatement ="select col1, 'col1''s text' from toto";
        Assert.assertEquals("select col1, 'col1''s text' from toto", PerfLogger.removeComments(sqlStatement));
    }

    @Test
    public void removeCommentsEndComments(){
        String sqlStatement ="select col1, 'col1''s text' from toto --teststs";
        Assert.assertEquals("select col1, 'col1''s text' from toto ", PerfLogger.removeComments(sqlStatement));
    }
    @Test
    public void removeCommentsEndCommentsMultiline(){
        String sqlStatement ="--first\n--second\nselect col1, 'col1''s /*in text*/ text' from toto --last";
        Assert.assertEquals("select col1, 'col1''s /*in text*/ text' from toto ", PerfLogger.removeComments(sqlStatement));
    }
    @Test
    public void removeCommentsBlockMultiline(){
        String sqlStatement ="/*first\nsecond\n*/select col1, 'col1''s /*in text*/ text' from toto --last";
        Assert.assertEquals("select col1, 'col1''s /*in text*/ text' from toto ", PerfLogger.removeComments(sqlStatement));
    }
}
