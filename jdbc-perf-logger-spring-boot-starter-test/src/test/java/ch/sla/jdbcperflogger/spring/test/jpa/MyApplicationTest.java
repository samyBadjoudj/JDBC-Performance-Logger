package ch.sla.jdbcperflogger.spring.test.jpa;

import static java.util.regex.Pattern.DOTALL;
import static java.util.regex.Pattern.MULTILINE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.hibernate.jpa.internal.EntityManagerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import ch.sla.jdbcperflogger.logger.PerfLoggerRemoting;
import ch.sla.jdbcperflogger.logger.RecordingLogSender;
import ch.sla.jdbcperflogger.model.StatementLog;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = MyApplication.class)
public class MyApplicationTest {
    private static final String SELECT_COUNT_FROM_PERSON = "SELECT COUNT(*) from PERSON";

    @Autowired
    private JdbcTemplate template;

    @Autowired
    EntityManager entityManager;

    private final RecordingLogSender logRecorder = new RecordingLogSender();

    @Before
    public void setup() throws Exception {
        PerfLoggerRemoting.addSender(logRecorder);
    }

    @After
    public void tearDown() throws Exception {
        PerfLoggerRemoting.removeSender(logRecorder);
    }

    @SuppressWarnings("null")
    @Test
    public void testDefaultSettings() throws Exception {
        assertEquals(new Integer(1), template.queryForObject(SELECT_COUNT_FROM_PERSON, Integer.class));
        final StatementLog log = (StatementLog) logRecorder.lastLogMessage(2);
        assertTrue(log.getRawSql().contains(SELECT_COUNT_FROM_PERSON));
    }

    @Test
    public void testWithComments(){
        String comments = "first param ?, second param in comment ?";
        //should use named param
        Query query = entityManager.createQuery("SELECT p from Person p where p.firstName =  ? ",Person.class)
                .setParameter(1,"David")
                .setHint("org.hibernate.comment", comments);
        query.getResultList();
        final StatementLog log = (StatementLog) logRecorder.lastLogMessage(2);
        // test that comments from hibernate are in raw sql not in compiled sql
        assertTrue(log.getRawSql().contains(comments));
        assertTrue(!log.getFilledSql().contains(comments));
    }

    private Pattern pattern = Pattern.compile("(?:'(?:''|[^'])*')|(?:--[^\\r\\n]*$)|(?:\\/\\*(?:[^\\*]|\\*(?!\\/))*\\*\\/)|(\\?)",MULTILINE | DOTALL);
    private final static String REPL = "___REPL___";

    @Test
    public void test() {
        String sql = "select  from table '--co''oo?' ? 'ti-- /* ti ? */' --toto  \n" +
                "2nd ligne /* comment */ lkjl /* comment\n" +
                "' ok ' * ok\n" +
                "on multilines */\n" +
                "where toto = ? kfjlskd";

        String replaced = replaceQuestionMarkForBindVariables(sql);

        System.out.println("FINAL sql: " + replaced);

        assertEquals("", replaceQuestionMarkForBindVariables(""));
        assertEquals(REPL, replaceQuestionMarkForBindVariables("?"));
        assertEquals(REPL + " + " + REPL, replaceQuestionMarkForBindVariables("? + ?"));
        assertEquals("select * from toto", replaceQuestionMarkForBindVariables("select * from toto"));
        assertEquals("select * from toto where name=" + REPL, replaceQuestionMarkForBindVariables("select * from toto where name=?"));
        assertEquals("select * from toto where name=" + REPL + " || '?'", replaceQuestionMarkForBindVariables("select * from toto where name=? || '?'"));
        assertEquals("select * from toto where name=" + REPL + " -- eol comment?", replaceQuestionMarkForBindVariables("select * from toto where name=? -- eol comment?"));
        assertEquals("select * from toto where name=" + REPL + " /*comment?*/", replaceQuestionMarkForBindVariables("select * from toto where name=? /*comment?*/"));
        assertEquals("select * from toto where name=" + REPL + " /*multiline\ncomment?*/", replaceQuestionMarkForBindVariables("select * from toto where name=? /*multiline\ncomment?*/"));
    }

    private String replaceQuestionMarkForBindVariables(String sql) {

        Matcher matcher = pattern.matcher(sql);

        StringBuilder strBuilder = new StringBuilder();

        int lastReplacementIndex = 0;

        while (matcher.find()) {
            if (matcher.group(1) != null) {
                strBuilder.append(sql.substring(lastReplacementIndex, matcher.start(1)));
                strBuilder.append(REPL);
                lastReplacementIndex = matcher.end(1);
            }
        }

        strBuilder.append(sql.substring(lastReplacementIndex, sql.length()));
        return strBuilder.toString();
    }


}
