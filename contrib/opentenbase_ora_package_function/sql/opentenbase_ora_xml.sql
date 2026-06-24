\c opentenbase_ora_package_function_regression_ora
\set ECHO none
CREATE EXTENSION IF NOT EXISTS opentenbase_ora_package_function;
set client_min_messages TO default;

CALL DBMS_OUTPUT.SERVEROUTPUT (true);

-- TEST XMLTYPE('')
DECLARE
    my_varchar2 VARCHAR2(1000);
    my_clob CLOB;
    my_xml XMLType;
BEGIN
    my_varchar2 := '<root><element1>XMLType VARCHAR2</element1><element2>XMLType VARCHAR2</element2></root>';
    my_clob := '<root><element1>XMLType CLOB</element1><element2>XMLType CLOB</element2></root>';
    my_xml := XMLType(my_varchar2);
    DBMS_OUTPUT.PUT_LINE('output my_varchar2 stringval: ' || my_xml.getStringVal());
    DBMS_OUTPUT.PUT_LINE('output my_varchar2 clobval: ' || my_xml.getClobVal());
    my_xml := XMLType(my_clob);
    DBMS_OUTPUT.PUT_LINE('output my_clob stringval: ' || my_xml.getStringVal());
    DBMS_OUTPUT.PUT_LINE('output my_clob clobval: ' || my_xml.getClobVal());

    my_xml := XMLType('<root>raw string</root>');
    DBMS_OUTPUT.PUT_LINE('output raw string: ' || my_xml.getStringVal());
END;
/

-- Test string functions about XMLType
SELECT XMLType('<NODE2>' || XMLType('<NODE><ele>1</ele></NODE>') || '</NODE2>') FROM DUAL;
SELECT XMLType(CONCAT('<NODE2>', CONCAT(XMLType('<NODE><ele>1</ele></NODE>'), '</NODE2>'))) FROM DUAL;
SELECT '<NODE2>' || XMLType('<NODE><ele>1</ele></NODE>') FROM DUAL;

-- LENGTH and SUBSTR not support now, need to add a xmltype->text cast, and address || operator conflict
SELECT LENGTH(XMLType('<NODE><ele>1</ele></NODE>')) FROM DUAL;
SELECT LENGTH(XMLType('<NODE>
                        <ele>1</ele>
                       </NODE>')) FROM DUAL;

SELECT LENGTH('<NODE2>' || XMLType('<NODE>
                        <ele>1</ele>
                       </NODE>')|| '</NODE2>') FROM DUAL;
SELECT SUBSTR(XMLType('<NODE2>' || XMLType('<NODE><ele>1</ele></NODE>') || '</NODE2>'), 1, 5) FROM DUAL;

-- Test extract and extractval

-- OpenTenBase cannot support func().func() call now
-- SELECT EXTRACT(XMLType('<employee><name>John Doe</name><age>30</age><department>IT</department></employee>'), '/employee/name/text()').getStringVal() AS name FROM DUAL;
SELECT EXTRACT(XMLType('<employee><name>John Doe</name><age>30</age><department>IT</department></employee>'), '/employee/name/text()') AS name FROM DUAL;
DECLARE
    xmlval XMLType;
BEGIN
    xmlval := EXTRACT(XMLType('<employee><name>John Doe</name><age>30</age><department>IT</department></employee>'), '/employee/name/text()');
    DBMS_OUTPUT.PUT_LINE(xmlval.getStringVal());
END;
/

SELECT EXTRACT(XMLType('<employees><employee><name>John</name><age>30</age><department>IT</department></employee><employee><name>Jane</name><age>28</age><department>HR</department></employee></employees>'), '/employees/employee')
    AS employee_data FROM DUAL;

DECLARE
    xmlval XMLType;
BEGIN
    xmlval := EXTRACT(XMLType('<employees><employee><name>John</name><age>30</age><department>IT</department></employee><employee><name>Jane</name><age>28</age><department>HR</department></employee></employees>'), '/employees/employee');
    DBMS_OUTPUT.PUT_LINE(xmlval.getStringVal());
END;
/

SELECT EXTRACT(XMLType('<employee><name id="1">John Doe</name><age>30</age><department>IT</department></employee>'), '/employee/name/@id') AS name_id FROM DUAL;

-- OpenTenBase success, OpenTenBase_Ora error
DECLARE
    xmlval XMLType := XMLType('<employees><employee><name>John</name><age>30</age><department>IT</department></employee><employee><name>Jane</name><age>28</age><department>HR</department></employee></employees>');
BEGIN
    xmlval := EXTRACT(xmlval, '/employees/employee');
    DBMS_OUTPUT.PUT_LINE(xmlval.getStringVal());
END;
/

DECLARE
    xmlval XMLType := XMLType('<a><f b1="b1" b2="b2">bbb1</f><b b1="b1" b2="b2">bbb2</b></a>');
    xmlres XMLType;
BEGIN
    xmlres := xmlval.EXTRACT('/a/f');
    DBMS_OUTPUT.PUT_LINE(xmlres.getStringVal());
END;
/

-- OpenTenBase report error, opentenbase_ora report error too.
SELECT EXTRACT('<employee><name id="1">John Doe</name><age>30</age><department>IT</department></employee>', '/employee/name/@id') AS name_id FROM DUAL;

-- Test extractvalue
SELECT EXTRACTVALUE(XMLType('<employee><name>John Doe</name><age>30</age><department>IT</department></employee>'), '/employee/name') AS name FROM DUAL;

-- OpenTenBase report error, opentenbase_ora report error too.
SELECT EXTRACTVALUE(XMLType('<employee><name>John Doe</name><age>30</age><department>IT</department></employee>'), '/employee') AS name FROM DUAL;
SELECT EXTRACTVALUE(XMLType('<employee><name id="1">John Doe</name><age>30</age><department>IT</department></employee>'), '/employee/name/@id') AS name_id FROM DUAL;

-- OpenTenBase success, opentenbase_ora error
SELECT EXTRACTVALUE('<employee><name id="1">John Doe</name><age>30</age><department>IT</department></employee>', '/employee/name/@id') AS name_id FROM DUAL;

-- OPENTENBASE_ORA REPORT ERROR, OPENTENBASE SUCCESS
DECLARE
    myvarchar VARCHAR2(1000);
BEGIN
    myvarchar := EXTRACTVALUE(XMLType('<employee><name>John Doe</name><age>30</age><department>IT</department></employee>'), '/employee/name');
    DBMS_OUTPUT.PUT_LINE(myvarchar);
END;
/

-- Test xmlsequence
DECLARE
    v_employees XMLType;
    v_employee_array XMLSequenceType;
BEGIN
    v_employees := XMLType('<employees><employee><name>Jane Smith</name><age>28</age><department>HR</department></employee><employee><name>John Doe</name><age>30</age><department>IT</department></employee></employees>');
    SELECT XMLSequence(EXTRACT(v_employees, '/employees/employee')) INTO v_employee_array FROM DUAL;

    DBMS_OUTPUT.PUT_LINE('Employee count: ' || v_employee_array.COUNT);

    FOR i IN 1 .. v_employee_array.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('Length of Employee ' || i || ': ' || LENGTH(v_employee_array(i).getStringVal()));
        DBMS_OUTPUT.PUT_LINE('Employee ' || i || ': ' || v_employee_array(i).getStringVal());
    END LOOP;
END;
/

-- In opentenbase_ora, you should replace VAL with COLUMN_VALUE
SELECT EXTRACT(COLUMN_VALUE, '/employee/name/text()') as employee_name
FROM (
         SELECT XMLType('<employees><employee><name>John Doe</name><age>30</age><department>IT</department></employee><employee><name>Jane Smith</name><age>28</age><department>HR</department></employee></employees>') as employee_data
         FROM dual
     ) x,
    TABLE(XMLSequence(EXTRACT(x.employee_data, '/employees/employee')));

-- In opentenbase_ora, you should replace e.VAL with e.COLUMN_VALUE
SELECT e.employee_name
FROM (
         SELECT XMLType('<employees><employee><name>John Doe</name><age>30</age><department>IT</department></employee><employee><name>Jane Smith</name><age>28</age><department>HR</department></employee></employees>') as employee_data
         FROM dual
     ) x,
     TABLE(XMLSequence(EXTRACT(x.employee_data, '/employees/employee'))) e,
     XMLTable('/employee' passing e.COLUMN_VALUE
         columns employee_name VARCHAR2(200) PATH 'name') e;

-- In opentenbase_ora, you should replace VAL with COLUMN_VALUE
DECLARE
    xml_data XMLType;
    employee_name VARCHAR2(200);
BEGIN
    xml_data := XMLType('<employees><employee><name>John Doe</name><age>30</age><department>IT</department></employee><employee><name>Jane Smith</name><age>28</age><department>HR</department></employee></employees>');

    FOR r IN (
        SELECT EXTRACT(COLUMN_VALUE, '/employee/name/text()') AS employee_name
        FROM TABLE(XMLSequence(EXTRACT(xml_data, '/employees/employee')))
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('Employee Name: ' || r.employee_name::VARCHAR);
    END LOOP;
END;
/

-- Test XMLELEMENT
-- XMLELEMENT does not support EVALNAME tag in OpenTenBase
SELECT XMLELEMENT("employee", 'John Doe') AS xml_output FROM DUAL;
SELECT XMLELEMENT("employee", XMLATTRIBUTES('1' AS "id"), 'John Doe') AS xml_output FROM DUAL;
SELECT XMLELEMENT("employee",
                  XMLATTRIBUTES('1' AS "id"),
                  XMLELEMENT("name", 'John Doe'),
                  XMLELEMENT("age", '30'),
                  XMLELEMENT("department", 'IT')
       ) AS xml_output
FROM DUAL;

DECLARE
    xml_output XMLType;
BEGIN
    SELECT XMLELEMENT("employee", 'John Doe') INTO xml_output FROM DUAL;
    DBMS_OUTPUT.PUT_LINE(xml_output.getStringVal());
    SELECT XMLELEMENT("employee", XMLATTRIBUTES('1' AS "id"), 'John Doe') INTO xml_output FROM DUAL;
    DBMS_OUTPUT.PUT_LINE(xml_output.getStringVal());
    SELECT XMLELEMENT("employee", XMLATTRIBUTES('1' AS "id"), XMLELEMENT("name", 'John Doe'), XMLELEMENT("age", '30'), XMLELEMENT("department", 'IT')) INTO xml_output FROM DUAL;
    DBMS_OUTPUT.PUT_LINE(xml_output.getStringVal());
END;
/

WITH xml_data AS (
    SELECT XMLELEMENT("employee",
                      XMLELEMENT("name", 'John Doe'),
                      XMLELEMENT("age", '30'),
                      XMLELEMENT("department", 'IT')
           ) AS xml_string
    FROM DUAL
)
SELECT XMLPARSE(CONTENT xml_string) AS xml_output FROM xml_data;

-- Test xmlagg
SELECT XMLELEMENT("employees",
                  XMLAGG(
                          XMLELEMENT("employee",
                                     XMLELEMENT("name", 'John Doe'),
                                     XMLELEMENT("age", '30'),
                                     XMLELEMENT("department", 'IT')
                          )
                  )
       ) AS xml_output
FROM DUAL;

CREATE TABLE employees_test_20240712 (name VARCHAR2(100), age NUMBER, department VARCHAR2(100));
INSERT INTO employees_test_20240712 (name, age, department) VALUES ('John Doe', 30, 'IT');
INSERT INTO employees_test_20240712 (name, age, department) VALUES ('Jane Smith', 28, 'HR');
SELECT XMLELEMENT("employees",
                  XMLAGG(
                          XMLELEMENT("employee",
                                     XMLELEMENT("name", name),
                                     XMLELEMENT("age", age),
                                     XMLELEMENT("department", department)
                          )
                  )
       ) AS xml_output
FROM employees_test_20240712;

DROP TABLE employees_test_20240712;

-- OpenTenBase can not do implicit cast when do assign in plsql
DECLARE
    employee1 XMLType;
    employee2 XMLType;
    employees_aggregated XMLType;
BEGIN
    SELECT XMLELEMENT("employee",
                    XMLELEMENT("name", 'John Doe'),
                    XMLELEMENT("age", '30'),
                    XMLELEMENT("department", 'IT')
                ) INTO employee1 FROM DUAL;

--     employee1 := XMLELEMENT("employee",
--                   XMLELEMENT("name", 'John Doe'),
--                   XMLELEMENT("age", '30'),
--                   XMLELEMENT("department", 'IT')
--                 );
    SELECT XMLELEMENT("employee",
                  XMLELEMENT("name", 'Jane Smith'),
                  XMLELEMENT("age", '28'),
                  XMLELEMENT("department", 'HR')
                ) INTO employee2 FROM DUAL;
    DBMS_OUTPUT.PUT_LINE('employee1: ' || employee1.getStringVal());
    DBMS_OUTPUT.PUT_LINE('employee2: ' || employee2.getStringVal());
    SELECT XMLELEMENT("employees", employee1, employee2) INTO employees_aggregated FROM DUAL;
    DBMS_OUTPUT.PUT_LINE('Aggregated XML: ' || employees_aggregated.getStringVal());
END;
/

-- In OpenTenBase_Ora, replace e.VAL with e.CLOUMN_VALUE
DECLARE
    xml_data XMLType;
    extract_res XMLType;
BEGIN
    SELECT XMLELEMENT("employees",
                XMLELEMENT("employee",
                    XMLELEMENT("name", 'John Doe'),
                    XMLELEMENT("age", '30'),
                    XMLELEMENT("department", 'IT')
                ),
                XMLELEMENT("employee",
                    XMLELEMENT("name", 'Jane Smith'),
                    XMLELEMENT("age", '28'),
                    XMLELEMENT("department", 'HR')
                )
       ) INTO xml_data FROM DUAL;

    SELECT EXTRACT(e.COLUMN_VALUE, '/employee/name[1]/text()') INTO extract_res FROM TABLE(XMLSequence(EXTRACT(xml_data, '/employees/employee[1]'))) e;
    DBMS_OUTPUT.PUT_LINE(extract_res.getStringVal());
END;
/

-- Test XMLAGG
CREATE TABLE xmlagg_test_20240712(department_id NUMBER, job_id VARCHAR2(1000), last_name VARCHAR2(1000));
INSERT INTO xmlagg_test_20240712 VALUES(30, 'PU_CLERK', 'Baida');
INSERT INTO xmlagg_test_20240712 VALUES(30, 'PU_CLERK', 'Himuro');
INSERT INTO xmlagg_test_20240712 VALUES(30, 'PU_CLERK', 'Tobias');
INSERT INTO xmlagg_test_20240712 VALUES(90, 'MK_REP', 'Fay');
INSERT INTO xmlagg_test_20240712 VALUES(30, 'PU_MAN', 'Raphaely');
INSERT INTO xmlagg_test_20240712 VALUES(30, 'PU_CLERK', 'Colmenares');
INSERT INTO xmlagg_test_20240712 VALUES(60, 'AD_ASST', 'Whalen');
INSERT INTO xmlagg_test_20240712 VALUES(30, 'PU_CLERK', 'Khoo');
INSERT INTO xmlagg_test_20240712 VALUES(90, 'MK_MAN', 'Hartstein');

SELECT XMLELEMENT("Department",
                  XMLAGG(XMLELEMENT("Employee", e.job_id||' '||e.last_name)
                      ORDER BY last_name))
           AS "Dept_list" FROM xmlagg_test_20240712 e  WHERE e.department_id = 30;

SELECT XMLELEMENT("Department",
                  XMLAGG(XMLELEMENT("Employee", e.job_id||' '||e.last_name)))
           AS "Dept_list"
FROM xmlagg_test_20240712 e
GROUP BY e.department_id;

-- xmlparse does not check the content validation when specify CONTENT tag(even if you do not specify a WELLFORMED tag)
DECLARE
    xmlagg_res XMLType;
BEGIN
    SELECT XMLAGG(XMLPARSE(CONTENT t.last_name || ',' WELLFORMED) ORDER BY t.department_id) INTO xmlagg_res FROM xmlagg_test_20240712 t;
    DBMS_OUTPUT.PUT_LINE(xmlagg_res.getClobVal());
END;
/

DECLARE
    xmlagg_res XMLType;
BEGIN
    SELECT XMLAGG(XMLPARSE(CONTENT t.last_name || ',') ORDER BY t.department_id) INTO xmlagg_res FROM xmlagg_test_20240712 t;
    DBMS_OUTPUT.PUT_LINE(xmlagg_res.getClobVal());
END;
/

-- xmlparse dwill check the document validation when specify DOCUMENT tag(even if you specify a WELLFORMED tag)
DECLARE
    xmlagg_res XMLType;
BEGIN
    SELECT XMLAGG(XMLPARSE(DOCUMENT t.last_name || ',' WELLFORMED) ORDER BY t.department_id) INTO xmlagg_res FROM (SELECT department_id, last_name FROM xmlagg_test_20240712 ORDER BY department_id, last_name) t;
    DBMS_OUTPUT.PUT_LINE(xmlagg_res.getClobVal());
END;
/

DECLARE
    xmlagg_res XMLType;
BEGIN
    SELECT XMLAGG(XMLPARSE(DOCUMENT t.last_name || ',') ORDER BY t.department_id) INTO xmlagg_res FROM (SELECT department_id, last_name FROM xmlagg_test_20240712 ORDER BY department_id, last_name) t;
    DBMS_OUTPUT.PUT_LINE(xmlagg_res.getClobVal());
END;
/

DROP TABLE xmlagg_test_20240712;

-- Test xmltable
SELECT *
FROM XMLTABLE(
             '/employees/employee' PASSING XMLTYPE('
          <employees>
               <employee>
                    <emp_id>1</emp_id>
                    <name>John Doe</name>
                    <position>Developer</position>
                    <salary>5000</salary>
               </employee>
               <employee>
                    <emp_id>2</emp_id>
                    <name>Jane Smith</name>
                    <position>Manager</position>
                    <salary>6500</salary>
               </employee>
          </employees>'
     )
     COLUMNS
             employee_id    INT           PATH 'emp_id',
             employee_name  VARCHAR2(100) PATH 'name',
             position       VARCHAR2(50)  PATH 'position',
             salary         NUMBER        PATH 'salary'
     ) AS emp_tab;

DECLARE
    xml_data XMLType := XMLType('<employees><employee><name>John Doe</name><age>30</age><department>IT</department></employee><employee><name>Jane Smith</name><age>28</age><department>HR</department></employee></employees>');
    TYPE employee_table_type IS TABLE OF VARCHAR2(100);
    employee_names employee_table_type;
BEGIN
    SELECT employee_name
           BULK COLLECT INTO employee_names
    FROM XMLTable('/employees/employee' PASSING xml_data
                COLUMNS employee_name VARCHAR2(100) PATH 'name');

    FOR i IN 1..employee_names.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('Employee Name: ' || employee_names(i));
    END LOOP;
END;
/

-- Test insert raw string to a XMLType column
CREATE TABLE xml_raw_string_insert_test20240712 (id NUMBER, c1 XMLType);
INSERT INTO xml_raw_string_insert_test20240712 VALUES (1, '<a>aaa</a>');
INSERT INTO xml_raw_string_insert_test20240712 VALUES (2, '<a><b>aaa</b><b>bbb</b></a>');
INSERT INTO xml_raw_string_insert_test20240712 VALUES (3, XMLType('<a a1="a1">aaa</a>'));
INSERT INTO xml_raw_string_insert_test20240712 VALUES (4, XMLType('<a><b b1="b1" b2="b2">bbb</b></a>'));
INSERT INTO xml_raw_string_insert_test20240712 VALUES (5, XMLType('<a><b b1="b1" b2="b2">bbb1</b><b b1="b1" b2="b2">bbb2</b></a>'));
INSERT INTO xml_raw_string_insert_test20240712 VALUES (6, XMLType('<a><f b1="b1" b2="b2">bbb1</f><b b1="b1" b2="b2">bbb2</b></a>'));
SELECT EXTRACT(c1, '/a/f') as RES FROM xml_raw_string_insert_test20240712 WHERE id = 6;
DROP TABLE xml_raw_string_insert_test20240712;

-- Test XMLSequenceType
SELECT * FROM TABLE(XMLSequenceType(EXTRACT(XMLType('
<employees>
   <employee>
        <id>1</id>
        <name>John Doe</name>
       <salary>50000</salary>
    </employee>
   <employee>
        <id>2</id>
        <name>Jane Smith</name>
       <salary>60000</salary>
    </employee>
</employees>'), '/employees/employee')));

-- Test XMLDOM
CREATE TABLE r_xmldom_20230315(id INT, send_text XMLType);
INSERT INTO r_xmldom_20230315 VALUES(1, '<STUDENTS><STUDENT id = "1"><NAME>Tom</NAME><GENDER>female</GENDER><SCORE><ENGLISH>99</ENGLISH><MATH>100</MATH></SCORE></STUDENT><STUDENT id = "2"><NAME>Jim</NAME><GENDER>male</GENDER><SCORE><ENGLISH>77</ENGLISH><MATH>93</MATH></SCORE></STUDENT></STUDENTS>');
CREATE TABLE r_xmldom_20230315_1(id INT, name VARCHAR2(10000), score INT);

-- from OpenTenBase_v5.06.4.900 fbc526af88a7eb066e18bcaf74cfa0996fc34ac0
CREATE OR REPLACE PROCEDURE p_xmldom_20230315 AS
    v_trans_text_info r_xmldom_20230315%ROWTYPE;
    v_text_detail r_xmldom_20230315_1%ROWTYPE;
    doc      XMLDOM.DOMDocument;
    node      XMLDOM.DOMNode;
    chilNodes XMLDOM.DOMNodeList;
    nodelist  XMLDOM.DOMNodelist;
    v_list_id VARCHAR(100);
    lenSdkatdrqx INTEGER;
    v_xml XMLType;
    v_clob CLOB;
    xmlPar XMLPARSER.Parser DEFAULT XMLPARSER.NEWPARSER;
BEGIN
    SELECT t.* INTO v_trans_text_info FROM r_xmldom_20230315 t WHERE t.id = 1;
    v_xml := v_trans_text_info.send_text;
    v_clob := v_xml.GetClobVal();
    xmlPar := XMLPARSER.newParser;
    XMLPARSER.parseClob(xmlPar, v_clob);
    doc := XMLPARSER.getDocument(xmlPar);
    xmlparser.freeParser(xmlPar);
    nodelist := XMLDOM.getElementsByTagName(doc, 'STUDENT');
    lenSdkatdrqx := XMLDOM.getLength(nodelist);
    FOR i IN 0..lenSdkatdrqx - 1 LOOP
        node := XMLDOM.item(nodelist, i);
        chilNodes :=  XMLDOM.getChildNodes(node);
        v_text_detail.id := i + 1;
        v_text_detail.name := XMLDOM.getNodeValue(xmldom.getFirstChild(xmldom.item(chilNodes, 0)));
        v_text_detail.score := XMLDOM.getNodeValue(xmldom.getFirstChild(xmldom.getFirstChild(xmldom.item(chilNodes, 2))));
        INSERT INTO r_xmldom_20230315_1 VALUES(v_text_detail.id, v_text_detail.name, v_text_detail.score);
    END LOOP;
  XMLDOM.freeDocument(doc);
END;
/

CALL p_xmldom_20230315();
SELECT * FROM r_xmldom_20230315_1 ORDER BY id;

DROP TABLE r_xmldom_20230315;
DROP TABLE r_xmldom_20230315_1;
DROP PROCEDURE p_xmldom_20230315;

DECLARE
    doc XMLDOM.DOMDocument;
    nodes XMLDOM.DOMNodeList;
    parser XMLPARSER.PARSER;
    stack TEXT;
    xml_data CLOB := '<employees>
           <employee>
                <name>John Doe</name>
               <salary>50000</salary>
            </employee>
           <employee>
                <name>Jane Smith</name>
               <salary>60000</salary>
            </employee>
        </employees>';
BEGIN
    parser := XMLPARSER.newParser;
    XMLPARSER.parseClob(parser, xml_data);
    doc := XMLPARSER.getDocument(parser);
    XMLPARSER.freeParser(parser);
    nodes := XMLDOM.getElementsByTagName(doc, 'employee');


    FOR i IN 1..XMLDOM.getLength(nodes) LOOP
        DBMS_OUTPUT.PUT_LINE('Employee ' || nodes(i)::TEXT);
    END LOOP;
    XMLDOM.freeDocument(doc);
END;
/

DECLARE
    l_doc XMLDOM.DOMDocument;
    l_nodes XMLDOM.DOMNodeList;
    l_childnodes XMLDOM.DOMNodeList;
    l_childnode XMLDOM.DOMNode;
    l_node XMLDOM.DOMNode;
    l_parser XMLPARSER.PARSER;
BEGIN
    l_parser := XMLPARSER.newParser;
    XMLPARSER.parseClob(l_parser, '
       <employees>
           <employee>
                <name>John Doe</name>
                <salary>50000</salary>
            </employee>
           <employee>
                <name>Jane Smith</name>
                <salary>60000</salary>
            </employee>
        </employees>
    ');
    l_doc := XMLPARSER.getDocument(l_parser);

    l_nodes := XMLDOM.getElementsByTagName(l_doc, 'employee');

    FOR i IN 1..XMLDOM.getLength(l_nodes) LOOP
        l_node := XMLDOM.item(l_nodes, i - 1);
        l_childnodes := XMLDOM.getChildNodes(l_node);
        FOR j IN 1..XMLDOM.getLength(l_childnodes) LOOP
            l_childnode := XMLDOM.item(l_childnodes, j - 1);
            DBMS_OUTPUT.PUT_LINE(XMLDOM.getNodeValue(l_childnode));
        END LOOP;
    END LOOP;
    XMLPARSER.freeParser(l_parser);
    XMLDOM.freeDocument(l_doc);
END;
/

-- from OpenTenBase_v5.06.4.900 51ea5594af4269a4ab59d831a39334de4479700e
--Bugfix:Fix when <label></label> does not contain a value, the xmltable extract
--       element reports an error, and the extractValue reports an error.
select TRIM(extractValue(a.column_value, '/ROW/AUTHTYPE')) auth_type
     ,TRIM(extractValue(a.column_value, '/ROW/CERTICODE')) certi_code
     ,TRIM(extractValue(a.column_value, '/ROW/REALNAME')) real_name
     ,TRIM(extractValue(a.column_value, '/ROW/ORGANID')) organ_id
     ,TRIM(extractValue(a.column_value, '/ROW/BSAPPLYID')) bs_apply_id
     ,TRIM(extractValue(a.column_value, '/ROW/USERID')) user_id
     ,TRIM(extractValue(a.column_value, '/ROW/AUTHID')) auth_id
     ,TRIM(extractValue(a.column_value, '/ROW/CERTITYPE')) certi_type

from xmltable('/ROWS/ROW' passing xmltype('<?xml version="1.0" encoding="UTF-8" ?>
<ROWS><ROW>
<AUTHTYPE>3</AUTHTYPE>
<CERTICODE>310107201312055718</CERTICODE>
<REALNAME>45441089</REALNAME>
<ORGANID></ORGANID>
<BSAPPLYID>13277767</BSAPPLYID>
<USERID>180121548</USERID>
<AUTHID>2784626</AUTHID>
<CERTITYPE>1</CERTITYPE>
</ROW></ROWS>')) a;

WITH sample_data AS (
    SELECT XMLTYPE('
    <employees>
      <employee>
        <id>1</id>
        <name>John Doe</name>
      </employee>
      <employee>
        <id>2</id>
        <name>Jane Smith</name>
      </employee>
    </employees>
  ') AS xml_data
    FROM dual
)
SELECT
    EXTRACT(VALUE(e), '/employee/id') AS employee_id,
    EXTRACT(VALUE(e), '/employee/name') AS employee_name
FROM sample_data,
     TABLE(XMLSEQUENCE(EXTRACT(xml_data, '/employees/employee'))) e;

WITH sample_data AS (
    SELECT XMLTYPE('
    <employees>
      <employee>
        <id>1</id>
        <name>John Doe</name>
      </employee>
      <employee>
        <id>2</id>
        <name>Jane Smith</name>
      </employee>
    </employees>
  ') AS xml_data
    FROM dual
)
SELECT
    EXTRACTVALUE(VALUE(e), '/employee/id') AS employee_id,
    EXTRACTVALUE(VALUE(e), '/employee/name') AS employee_name
FROM sample_data,
     TABLE(XMLSEQUENCE(EXTRACT(xml_data, '/employees/employee'))) e;

CREATE OR REPLACE PACKAGE test_pkg_20240718 AS
 PROCEDURE JLWJ_ASYN_WAIT_SEND_BACK(
        I_RESP_MESSAGE IN CLOB
);
END;
/

CREATE OR REPLACE PACKAGE body test_pkg_20240718 AS
PROCEDURE JLWJ_ASYN_WAIT_SEND_BACK(
        I_RESP_MESSAGE IN CLOB
)
IS
DECLARE
XML_STRING VARCHAR2(200):= '<employee><name>Johb</name></employee>';
        -- 解析xml文档
        V_DATA_XML XMLTYPE;
        -- 汇总信息
CURSOR C_REPLAY_MSG(DATAIL XMLTYPE) IS
SELECT TRIM(EXTRACTVALUE(VALUE(A), 'Message/Head/UploadSuccess')) AS UPLOADSUCCESS
     , TRIM(EXTRACTVALUE(VALUE(A), 'Message/Body/SumError')) AS SUMERROR
     , TRIM(EXTRACTVALUE(VALUE(A), 'Message/Body/BatchNo')) AS BATCHNO
FROM TABLE(XMLSEQUENCE(EXTRACT(DATAIL, 'MeSSage'))) A;
BEGIN
OPEN C_REPLAY_MSG(XMLTYPE('<employee><name>Johb</name></employee>'));
CLOSE  C_REPLAY_MSG;
END;
END;
/
call test_pkg_20240718.JLWJ_ASYN_WAIT_SEND_BACK('');
drop package test_pkg_20240718;

SELECT EXTRACT(VALUE (A), 'request/detail/productInfoList/productInfo') AS LIST_ROW
    FROM TABLE (XMLSEQUENCE (EXTRACT (XMLType('<productInfo><request>1</request></productInfo>'), 'request'))) A;

SELECT *
    FROM TABLE (XMLSEQUENCE (EXTRACT (XMLType('<productInfo><request>1</request></productInfo>'), '/request'))) A;

SELECT * FROM TABLE (XMLSEQUENCE (EXTRACT (XMLType('<productInfo><request>1</request></productInfo>'), '/request'))) A;

SELECT XMLSEQUENCE(EXTRACT (XMLType('<productInfo><request>1</request></productInfo>'), 'request')) AS val FROM DUAL;

SELECT EXTRACTVALUE(VALUE (A), 'request/detail/productInfoList/productInfo') AS LIST_ROW
    FROM TABLE (XMLSEQUENCE (EXTRACT (XMLType('<productInfo><request>1</request></productInfo>'), 'request'))) A;

DECLARE
    xdata XMLType := XMLType('<root></root>');
BEGIN
    xdata.val := NULL;
    DBMS_OUTPUT.PUT_LINE(xdata.getStringVal());
END;
/

DECLARE
xdata XMLType := XMLType('<root></root>');
BEGIN
    xdata.val := NULL;
    DBMS_OUTPUT.PUT_LINE(xdata.getClobVal());
END;
/

SELECT TRIM(EXTRACTVALUE(VALUE(MESS), '/rcd')) FROM TABLE(XMLSEQUENCE(EXTRACT(XMLTYPE('<?xml version="1.0" encoding="utf-8"?><rcd>900002</rcd>'), 'rcd'))) MESS;

SELECT EXTRACT(XMLTYPE('<?xml version="1.0" encoding="utf-8"?><abc><rcd>900002</rcd></abc>'), 'rcd');
SELECT EXTRACTVALUE(XMLTYPE('<?xml version="1.0" encoding="utf-8"?><rcd>900002</rcd>'), 'rcd');

-- start TAPD: 127903831
SELECT * FROM TABLE(XMLSEQUENCE(EXTRACT(xmltype('<root><parent_1 gid="1" id="1"><idx>1</idx><parent_2 gid="2" id="2"><idx>2</idx><parent_3 gid="3" id="3"><idx>3</idx><parent_4 gid="4" id="4"><idx>4</idx></parent_4></parent_3></parent_2></parent_1></root>'), '//@id')));

SELECT XMLSEQUENCE(EXTRACT(xmltype('<root><parent_1 gid="1" id="1"><idx>1</idx><parent_2 gid="2" id="2"><idx>2</idx><parent_3 gid="3" id="3"><idx>3</idx><parent_4 gid="4" id="4"><idx>4</idx></parent_4></parent_3></parent_2></parent_1></root>'), '//@id')) FROM DUAL;

SELECT EXTRACT(xmltype('<root><parent_1 gid="1" id="1"><idx>1</idx><parent_2 gid="2" id="2"><idx>2</idx><parent_3 gid="3" id="3"><idx>3</idx><parent_4 gid="4" id="4"><idx>4</idx></parent_4></parent_3></parent_2></parent_1></root>'), '//@id') FROM DUAL;

SELECT XMLSEQUENCE(EXTRACT(xmltype('<root><parent_1 gid="1" id="1"><idx>1</idx><parent_2 gid="2" id="2"><idx>2</idx><parent_3 gid="3" id="3"><idx>3</idx><parent_4 gid="4" id="4"><idx>4</idx></parent_4></parent_3></parent_2></parent_1></root>'), '//@id')) FROM DUAL;

SELECT EXTRACT(xmltype('<root><parent_1 gid="1" id="1">parent<idx>1</idx><parent_2 gid="2" id="2"><idx>2</idx><parent_3 gid="3" id="3"><idx>3</idx><parent_4 gid="4" id="4"><idx>4</idx></parent_4></parent_3></parent_2></parent_1></root>'), '/root/*') FROM DUAL;

SELECT XMLSEQUENCE(EXTRACT(xmltype('<root>root<parent_1 gid="1" id="1">parent1<idx>1</idx></parent_1><parent_2 gid="2" id="2"><idx>2</idx>parent2</parent_2></root>'), '/root/*|/root/text()')) FROM DUAL;

SELECT * FROM TABLE(XMLSEQUENCE(EXTRACT(xmltype('<root>root<parent_1 gid="1" id="1">parent1<idx>1</idx></parent_1><parent_2 gid="2" id="2"><idx>2</idx>parent2</parent_2></root>'), '/root/*|/root/text()')));

create table xmlsequence_bug_127903831(xm_col_xmltype xmltype);
insert into xmlsequence_bug_127903831 values(xmltype('<root><parent_1 gid="1" id="1"><idx>1</idx><parent_2 gid="2" id="2"><idx>2</idx><parent_3 gid="3" id="3"><idx>3</idx><parent_4 gid="4" id="4"><idx>4</idx></parent_4></parent_3></parent_2></parent_1></root>'));
select count(*) from xmlsequence_bug_127903831;
select EXTRACT(x.xm_col_xmltype, '//@id') "attribute Val List" from xmlsequence_bug_127903831 x;

create view xmlsequence_bug_127903831_v1 as
SELECT t.column_value "attribute Val"
FROM xmlsequence_bug_127903831 x,
     TABLE(XMLSEQUENCE(EXTRACT(x.xm_col_xmltype, '//@id'))) t;
select * from xmlsequence_bug_127903831_v1;

drop view xmlsequence_bug_127903831_v1;
drop table xmlsequence_bug_127903831;
-- end TAPD: 127903831

--start TAPD: 127890235
SELECT EXTRACTVALUE(XMLTYPE('<string id="1"><![CDATA[value 腾讯<"北京" & "成都" > ]]></string>'),'/string') AS VAL FROM DUAL;
SELECT EXTRACTVALUE(XMLTYPE('<string id="1"><![CDATA[value 腾讯<"北京" & "成都" > ]]></string>'),'string/text()') AS VAL FROM DUAL;
SELECT EXTRACTVALUE(XMLTYPE('<string id="1"><![CDATA[value 腾讯<"北京" & "成都" > ]]><tencent>home</tencent></string>'),'string/text()') AS VAL FROM DUAL;
SELECT EXTRACTVALUE(XMLTYPE('<string id="1"><![CDATA[value 腾讯<"北京" & "成都" > ]]>123<tencent>home</tencent></string>'),'string/text()') AS VAL FROM DUAL;
SELECT EXTRACTVALUE(XMLTYPE('<string id="1"><![CDATA[value 腾讯<"北京" & "成都" > ]]>123<tencent>home</tencent></string>'),'string/tencent') AS VAL FROM DUAL;

-- comment will be ignored
SELECT EXTRACTVALUE(XMLTYPE('<string id="1" name="mict"><!-- This is a comment -->mystring</string>'),'string') AS VAL FROM DUAL;
SELECT EXTRACTVALUE(XMLTYPE('<string id="1" name="mict"><!-- This is a comment -->mystring</string>'),'string/text()') AS VAL FROM DUAL;

-- CDATA and text and comment
SELECT EXTRACTVALUE(XMLTYPE('<string id="1"><![CDATA[value 腾讯<"北京" & "成都" > ]]>123<![CDATA[value 腾讯<"北京" & "成 都" > ]]>  <!-- This is a comment --></string>'),'string') AS VAL FROM DUAL;
SELECT EXTRACTVALUE(XMLTYPE('<string id="1"><![CDATA[value 腾讯<"北京" & "成都" > ]]>123<![CDATA[value 腾讯<"北京" & "成 都" > ]]>  <!-- This is a comment --></string>'),'.') AS VAL FROM DUAL;

-- stylesheet will be ignored
SELECT EXTRACTVALUE(XMLTYPE('<string id="1"><?xml-stylesheet type="text/xsl" href="style.xsl"?>tencent<![CDATA[value<beijing> ]]></string>'),'string') AS VAL FROM DUAL;
SELECT EXTRACTVALUE(XMLTYPE('<string id="2" name="attr"><![CDATA[value<beijing> ]]><?xml-stylesheet type="text/xsl" href="style.xsl"?>tencent<![CDATA[value<beijing> ]]></string>'),'string') AS VAL FROM DUAL;

SELECT EXTRACTVALUE(XMLTYPE('<string id="2" name="attr"><![CDATA[Text content]]></string>'),'string') AS VAL FROM DUAL;

SELECT EXTRACTVALUE(XMLTYPE('<string id="1"><!-- This is a comment -->sxa</string>'),'string') AS VAL FROM DUAL;

-- select attribute
SELECT EXTRACTVALUE(XMLTYPE('<bookstore name="Xinhua Book Store"><book id="1" name="Peace and Love"><name></name><price></price></book></bookstore>'),'//@name') AS VAL FROM DUAL;
SELECT EXTRACTVALUE(XMLTYPE('<bookstore name="Xinhua Book Store"><book id="1" name="Peace and Love"><name></name><price></price></book></bookstore>'),'/@name') AS VAL FROM DUAL;
SELECT EXTRACTVALUE(XMLTYPE('<bookstore name="Xinhua Book Store"><book id="1" name="Peace and Love"><name></name><price></price></book></bookstore>'),'/bookstore/book/@name') AS VAL FROM DUAL;

SELECT EXTRACTVALUE(XMLTYPE('<bookstore name="Xinhua Book Store"><book id="1" name="Peace and Love"><name></name><price></price></book></bookstore>'),'.') AS VAL FROM DUAL;
SELECT EXTRACTVALUE(XMLTYPE('<bookstore name="Xinhua Book Store"><book id="1" name="Peace and Love"><name></name><price></price></book></bookstore>'),'bookstore/book[1]/price') AS VAL FROM DUAL;
SELECT EXTRACTVALUE(XMLTYPE('<bookstore name="Xinhua Book Store">
                            <book id="1" name="Peace and Love"><name></name><price>28</price></book>
                            <book id="2" name="Lake Wall"><name></name><price>32</price></book>
                            </bookstore>'),'bookstore/book[price>30]/@name') AS VAL FROM DUAL;

--end TAPD: 127890235

--start TAPD: 127937301
SELECT t.column_value FROM TABLE(XMLSequence(extract(XMLTYPE('<root><parent_1 gid="1" id="1"><idx>1</idx><parent_2 gid="2" id="2"><idx>2</idx></parent_2></parent_1></root>'),'//idx'))) t;

SELECT extractvalue(t.column_value, 'idx') "val" FROM TABLE(XMLSequence(extract(XMLTYPE('<root><parent_1 gid="1" id="1"><idx>1</idx><parent_2 gid="2" id="2"><idx>2</idx></parent_2></parent_1></root>'),'//idx'))) t;
--end TAPD: 127937301

--start TAPD: 118980757
SELECT TRIM(EXTRACTVALUE(VALUE(A),'request/transAmt')) transAmt,
       TRIM(EXTRACTVALUE(VALUE(A),'request/accountName'))accountName,
       TRIM(EXTRACTVALUE (VALUE (A),'request/accountNo'))accountNo
FROM TABLE(XMLSEQUENCE(EXTRACT(XMLTYPE('<?xml version="1.0" encoding="GBK"?>
                                            <request>
                                                <merId>000092305234085</merId>
                                                <version>20180808</version>
                                                <method>2</method>
                                                <transDate>20240801</transDate>
                                                <orderNo>12110013120</orderNo>
                                                <curyId>CNY</curyId>
                                                <transAmt>2000</transAmt>
                                                <openBank>0308</openBank>
                                                <subBank></subBank>
                                                <unionBankNo></unionBankNo>
                                                <city>上海</city>
                                                <flag>00</flag>
                                                <purpose>太平养老付费</purpose>
                                                <privl></privl>
                                                <payMode>1</payMode>
                                                <accountNo>621152625152458456</accountNo>
                                                <accountName>下山</accountName>
                                                <mobileForBank></mobileForBank>
                                                <busiProFlag></busiProFlag>
                                                <relAccountNo>6668988</relAccountNo>
                                                <relAccountName>7715688</relAccountName>
                                                <termType>07</termType>
                                                <terminalId></terminalId>
                                                <userId></userId>
                                                <userRegisterTime></userRegisterTime>
                                                <userMail></userMail>
                                                <userMobile></userMobile>
                                                <diskSn></diskSn>
                                                <baseStationSn></baseStationSn>
                                                <codeInputType></codeInputType>
                                                <desc></desc>
                                            </request>'), 'request'))) A;
SELECT EXTRACT(XMLTYPE('<?xml version="1.0" encoding="GB2312"?><head>你好</head>'), '/head') FROM DUAL;
SELECT EXTRACT(XMLTYPE('<?xml version="1.0" encoding="GB2312" standalone="yes" ?><head>你好</head>'), '/head') FROM DUAL;
SELECT EXTRACT(XMLTYPE('<?xml version="1.0" encoding="GB18030"?><head>你好</head>'), '/head') FROM DUAL;
SELECT EXTRACT(XMLTYPE('<?xml version="1.0" encoding="UTF-8"?><head>你好</head>'), '/head') FROM DUAL;
SELECT EXTRACT(XMLTYPE('<?xml version="1.0" encoding="GBK"?><head>你好</head>'), '/head') FROM DUAL;
--end TAPD: 118980757

--start TAPD: 118980757
declare
vXml xmltype := xmltype('<company><dept id="1"><name>CSIG</name><employees><employee><id>1</id><name>W</name></employee><employee><id>2</id><name>w2</name></employee></employees></dept></company>');
    vString1 clob;
    vString2 clob;
    vString3 clob;
    vNumber1 number;
    vNumber2 number;
    vNumber3 number;
begin
    dbms_output.put_line(' ---- TABLE ----  ');
for x in (
        SELECT
            c."d_id" dept_id,
            c."d_name" dept_name,
            e."e_id" empoyee_id,
            e."e_name" employee_name
        FROM
            XMLTABLE('/company/dept'
                PASSING vXml
                COLUMNS
                    "d_id" varchar2(100) PATH '@id',
                    "d_name" varchar2(6) PATH 'name',
                    employees xmltype PATH 'employees'
            ) c,
            XMLTABLE('/employees/employee'
                PASSING c.employees
                COLUMNS
                    "e_id" varchar2(6) PATH 'id',
                    "e_name" varchar2(6) PATH 'name'
            ) e
    )
    loop
        dbms_output.put_line(x.dept_id || ' ---- '||x.dept_name||' : eid:'||x.empoyee_id||' ename:'||x.employee_name);
end loop;
end;
/
--end TAPD: 118980757

--start TAPD: 128074351
DECLARE
    xmlagg_res XMLType;
BEGIN
    SELECT XMLAGG(XMLPARSE(CONTENT 'a' || '<' WELLFORMED)) INTO xmlagg_res FROM dual ;
    DBMS_OUTPUT.PUT_LINE(xmlagg_res.getClobVal());
END;
/

DECLARE
    xmlagg_res XMLType;
BEGIN
    SELECT XMLAGG(XMLPARSE(CONTENT 'a' || '<'  )  ) INTO xmlagg_res FROM dual ;
    DBMS_OUTPUT.PUT_LINE(xmlagg_res.getClobVal());
EXCEPTION WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('error out (Expected)' || SQLERRM);
END;
/
--end TAPD: 128074351

--start TAPD: 127959761
declare
  xmlstring clob;
  ans varchar2(4000);
  vNumber number ;

  parser1 xmlparser.parser;
  vDoc xmldom.domdocument;
  vNodeList1 xmldom.domnodelist;
  vNodeList2 xmldom.domnodelist;
  vNodeList3 xmldom.domnodelist;
  vNode1 xmldom.domnode;
  vNode2 xmldom.domnode;
  vChildNode1 xmldom.domnode;
  vChildNode2 xmldom.domnode;
  vElem1 xmldom.domelement;
  vElem2 xmldom.domelement;
  vClob clob;
  vNumber1 number := 0;
  vNumber2 number := 0;
  vString varchar2(4000);

begin
  xmlstring := '<employees><emp id="1" gid="1"><id>1</id><f_name>W_1</f_name><l_name>Q_1</l_name><GENDER>F</GENDER></emp><emp id="2" gid="2"><id>2</id><f_name>W_2</f_name><l_name>Q_2</l_name><GENDER>M</GENDER></emp></employees>';

  parser1 := xmlparser.newParser;
  xmlparser.parseClob(parser1,xmlstring);
  vDoc := xmlparser.getDocument(parser1);

  vNodeList1 := xmldom.getElementsByTagName(vDoc,'emp');
  vNode1 := xmldom.item(vNodeList1, 0);
  vNodeList2 := xmldom.getChildNodes(vNode1);
for j in 0..(xmldom.getLength(vNodeList2) - 1)
    LOOP
      vChildNode1 := xmldom.item(vNodeList2, j);
      dbms_output.put('Element:'||xmldom.getNodeName(vChildNode1));
      vNodeList3 := xmldom.getChildNodes(vChildNode1);
      vChildNode1 := xmldom.item(vNodeList3,0);
      dbms_output.put_line('   Value: '|| xmldom.getNodeValue(vChildNode1) );
      dbms_output.put_line(' ----  ');
end loop;

  dbms_output.put_line(vNumber);
end;
/
--end TAPD: 127959761

--start TAPD: 127988387
create table t1_127988387(t_id number, t_name varchar2(200));
insert into t1_127988387 values(1,'emp-1');
insert into t1_127988387 values(2,'emp-2');

select length(t.empList)  from (
                                   SELECT XMLELEMENT("Department",
                                                     XMLAGG(XMLELEMENT("Employee", e.t_id||' '||e.t_name) ORDER BY e.t_id desc)
                                          )
                                              as empList
                                   FROM t1_127988387 e
                               ) t ;
drop table t1_127988387;
--end TAPD: 127988387

--start TAPD: 128320931
drop table test_table_128320931;
create table test_table_128320931(w_zone number, w_docks number);
insert into test_table_128320931 values(1,10);
insert into test_table_128320931 values(2,20);

declare
    vXML xmltype;
begin
    for x in (SELECT
        XMLELEMENT("Zone",
            XMLELEMENT("Name",'zone_'||w_zone ),
            XMLELEMENT("warehouses",
                xmlagg(
                    xmlelement("warehouse",
                        XMLELEMENT("docks", w_docks)
                    )
                )
            )
        ) vXml
        FROM test_table_128320931
        WHERE w_zone < 3
        GROUP BY w_zone
        ORDER BY w_zone
    )
    loop
        vXml := x.vXml;
        dbms_output.put_line(x.vXml::TEXT);
        dbms_output.put_line(vXml::TEXT);
    end loop;
end;
/
drop table test_table_128320931;
--end TAPD: 128320931


DECLARE
    vSeqType XMLSequenceType;
    vXml xmltype := xmltype('<root>
                                <resp>
                                      <code>200</code>
                                      <stat>OK</stat>
                                      <rcvseqno>1</rcvseqno>
                                      <sndseqno>2</sndseqno>
                                      <detail>n/a</detail>
                                </resp>
                                <resp>
                                      <code>500</code>
                                      <stat>fault</stat>
                                      <rcvseqno>10</rcvseqno>
                                      <sndseqno>20</sndseqno>
                                      <detail>invalid request</detail>
                                </resp>
                            </root>');
    vXml2 xmltype;
BEGIN
    dbms_output.put_line('______________________');
    SELECT EXTRACT(vXml, '/root/resp/code/text()') INTO vXml2 FROM DUAL;
    dbms_output.put_line('Extract:'||vXml2.getStringVal());

    SELECT XMLSequence(EXTRACT(vXml, '/root/resp/code')) INTO vSeqType FROM DUAL;
    dbms_output.put_line('# of xmlsequence:'||vSeqType.COUNT);
    FOR i IN 1 .. vSeqType.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('XmlSequence(' || i || '): ' || vSeqType(i).getStringVal());
    END LOOP;

    SELECT XMLSequence(EXTRACT(vXml, '/root/resp/code/text()')) INTO vSeqType FROM DUAL;
    dbms_output.put_line('# of xmlsequence:'||vSeqType.COUNT);
    FOR i IN 1 .. vSeqType.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('XmlSequence(' || i || '): ' || vSeqType(i).getStringVal());
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('error out(Unexpected) - ' || SQLERRM);
END;
/

DECLARE
    parser1 XMLParser.Parser;
    vDoc XMLDOM.DOMDocument;
    authorElements XMLDOM.DOMNodeList;
    bookcodeElements XMLDOM.DOMNodeList;
    authorNodeChildren XMLDOM.DOMNodeList;
    authorNode XMLDOM.DOMNode;
    bookcodeNode XMLDOM.DOMNode;
    i NUMBER;
    xmlClob CLOB;
    node XMLDOM.DOMNODE;
    node_value VARCHAR2(4000);

BEGIN
    -- 初始化XML CLOB数据
    xmlClob := '<books>
               <book>
                 <bookcode>B001</bookcode>
                 <title>Book Title 1</title>
                 <author>Author 1</author>
               </book>
               <book>
                 <bookcode>B002</bookcode>
                 <title>Book Title 2</title>
                 <author>Author 2</author>
               </book>
               <book>
                 <bookcode>B003</bookcode>
                 <title>Book Title 3</title>
                 <author>Author 3</author>
               </book>
             </books>';
    parser1 := XMLParser.newParser;
    XMLParser.parseClob(parser1, xmlClob);
    vDoc := XMLParser.getDocument(parser1);
    authorElements := XMLDOM.getElementsByTagName(vDoc, 'author');
    bookcodeElements := XMLDOM.getElementsByTagName(vDoc, 'bookcode');
    DBMS_OUTPUT.PUT_LINE('# of author:' || XMLDOM.getLength(authorElements));
	authorNode :=xmldom.item(authorElements,0);
    DBMS_OUTPUT.PUT_LINE('value of author[0]:' || xmldom.getNodeValue(authorNode));

    DBMS_OUTPUT.PUT_LINE('（OpenTenBase_Ora) value of author[0]:' || xmldom.getNodeValue(xmldom.item(xmldom.getChildNodes(authorNode),0)));

    xmldom.freeDocument(vDoc);
    XMLParser.freeParser(parser1);
END;
/

select xmlelement("ro<or",'value<') from dual;
drop table testxml_20240809;
create table testxml_20240809(x_id int, x_xml xmltype);
insert into testxml_20240809 values(1,xmlelement("ro<or",'value'));
insert into testxml_20240809 values(1, xmltype('<ro<or>value</ro<or>'));

select * from testxml_20240809;
drop table testxml_20240809;

DECLARE
    xml XMLType;
BEGIN
    SELECT EXTRACT(XMLType('<head><info>1</info></head>'), '/head') INTO xml FROM DUAL;
END;
/

CREATE OR REPLACE PROCEDURE TEST_XML_ANALYZE_20240809(V1 IN CLOB) AS
    V_DATE_XML XMLTYPE;
    LIST_ROW XMLTYPE;
    -- 定义第一个游标，用于提取XML中的head节点
    CURSOR C1(HEAD XMLTYPE) IS
        SELECT EXTRACT(VALUE(A), '/head/infolist/info') AS LIST_ROW
        FROM TABLE(XMLSEQUENCE(EXTRACT(HEAD, '/head'))) A;

    -- 定义第二个游标，用于提取info节点中的age信息
    CURSOR C2(LIST_ROW XMLTYPE) IS
        SELECT
            TRIM(EXTRACTVALUE(VALUE(A), 'info/name')) AS info_name,
            TRIM(EXTRACTVALUE(VALUE(A), 'info/age')) AS info_age
        FROM TABLE(XMLSEQUENCE(EXTRACT(LIST_ROW, 'info'))) A;
BEGIN
    -- 将输入的CLOB转换成XMLTYPE
    SELECT XMLTYPE(V1) INTO V_DATE_XML FROM DUAL;

    DBMS_OUTPUT.PUT_LINE('开始解析XML');

    -- 遍历第一个游标
    FOR REQ IN C1(V_DATE_XML) LOOP
        DBMS_OUTPUT.PUT_LINE('解析head节点');
        -- 打印head节点的内容
        LIST_ROW := REQ.LIST_ROW;
        DBMS_OUTPUT.PUT_LINE(LIST_ROW.getClobVal());

        -- 遍历第二个游标
        FOR PROD IN C2(REQ.LIST_ROW) LOOP
	        DBMS_OUTPUT.PUT_LINE('解析name/age节点');
            -- 打印info节点中的age信息
	        DBMS_OUTPUT.PUT_LINE('姓名: ' || PROD.info_name);
            DBMS_OUTPUT.PUT_LINE('年龄: ' || PROD.info_age);
        END LOOP;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('XML解析完成');
END;
/

CALL TEST_XML_ANALYZE_20240809('<head>
						<infolist>
							<info>
								<name>1</name>
								<age>2</age>
							</info>
							<info>
								<name>3</name>
								<age>4</age>
							</info>
						</infolist>
					</head>');

-- start TAPD: 129274147
WITH sample_data AS (
    SELECT XMLType('
    <root>
      <element><![CDATA[This is a CDATA section]]></element>
    </root>
  ') AS xml_data
    FROM dual
)
SELECT
    substr(cdata_value,1,20)
FROM sample_data t,
    XMLTABLE(
            '/root/element' PASSING t.xml_data
        COLUMNS cdata_value VARCHAR2(100) PATH 'text()'
    );
-- end TAPD: 129274147

DECLARE
xmlStr VARCHAR2(4000) := '<root ID="123">Sample text
    <![CDATA[Sample CDATA]]>
<!--Sample comment-->    </root>';
  xmlDoc XMLDOM.DOMDocument;
  textNode XMLDOM.DOMNode;
  cdataNode XMLDOM.DOMNode;
  commentNode XMLDOM.DOMNode;
  elementNode XMLDOM.DOMNode;
  parser XMLPARSER.PARSER;
BEGIN
    parser := XMLPARSER.newParser;
    XMLPARSER.parseClob(parser, xmlStr);
    xmlDoc := XMLPARSER.getDocument(parser);

    DBMS_OUTPUT.PUT_LINE('Length: ' || XMLDOM.getLength(XMLDOM.GetChildNodes(XMLDOM.ITEM(XMLDOM.getElementsByTagName(xmlDoc, 'root'), 0))));
  -- 获取文本节点并获取其值
  textNode := XMLDOM.ITEM(XMLDOM.GetChildNodes(XMLDOM.ITEM(XMLDOM.getElementsByTagName(xmlDoc, 'root'), 0)), 0);
  DBMS_OUTPUT.PUT_LINE('Text node value: ' || XMLDOM.getNodeValue(textNode));

  -- 获取CDATA节点并获取其值
  textNode := XMLDOM.ITEM(XMLDOM.GetChildNodes(XMLDOM.ITEM(XMLDOM.getElementsByTagName(xmlDoc, 'root'), 0)), 1);
  DBMS_OUTPUT.PUT_LINE('CDATA node value: ' || XMLDOM.getNodeValue(cdataNode));

  -- 获取注释节点并获取其值
  textNode := XMLDOM.ITEM(XMLDOM.GetChildNodes(XMLDOM.ITEM(XMLDOM.getElementsByTagName(xmlDoc, 'root'), 0)), 2);
  DBMS_OUTPUT.PUT_LINE('Comment node value: ' || XMLDOM.getNodeValue(commentNode));

  -- 获取元素节点的值
    elementNode := XMLDOM.ITEM(XMLDOM.getElementsByTagName(xmlDoc, 'root'), 0);
  DBMS_OUTPUT.PUT_LINE('Element node value: ' || XMLDOM.getNodeValue(elementNode));
END;
/

select * from XMLTABLE('/company/dept'
                PASSING xmltype('<company><dept id="1"><name>CSIG</name><employees>   <employee>   <id>1</id>   <name>W</name></employee><employee>   <id>2</id>   <name>w2</name></employee></employees></dept></company>')
                COLUMNS
                    "d_id" varchar2(100) PATH '@id',
                    "d_name" varchar2(6) PATH 'name',
                    employees xmltype PATH 'employees'
            );

-- 128171263
SELECT extractvalue(column_value, '/emp/id') "empId"
  FROM TABLE(XMLSequence(extract(XMLTYPE('<employees><emp><id>1</id></emp><emp><id>2</id></emp></employees>'),'/employees/emp'))) t;


-- test xml cat text
SELECT '<root>value</root>'::XML || '<root>value</root>'::TEXT;
SELECT '<root>value</root>'::XML || '<root>value</root>'::XML;
SELECT '<root>value</root>'::TEXT || '<root>value</root>'::XML;

create table xmlagg_table_warehouse_20240726(
  w_id int,
  w_name varchar2(100),
  w_spec clob,
  w_spec_xml xmltype
) PARTITION BY RANGE (w_id)
(PARTITION xmlagg_table_warehouse_20240726_p1 VALUES LESS THAN (100),
 PARTITION xmlagg_table_warehouse_20240726_p2 VALUES LESS THAN (200),
 PARTITION xmlagg_table_warehouse_20240726_p3 VALUES LESS THAN (300),
 PARTITION xmlagg_table_warehouse_20240726_p4 VALUES LESS THAN (400),
 PARTITION xmlagg_table_warehouse_20240726_p5 VALUES LESS THAN (MAXVALUE)
 ) distribute by replication;

create or replace procedure xmlagg_pl_dataseeding_20240726(dSize number, ns number)
is
  xmlString clob;
  batchSize number := 10;
  batchIdx number := 1;
  gender char := 'M';
begin
  for x in 1..dSize
  loop
    batchIdx := mod(x,batchSize);
    if mod(x,2) = 0 then
      gender := 'M';
    else
      gender := 'F';
    end if;
    xmlString := '<emp id="'||x||'" gid="'||batchIdx||'">'
              ||'<id>'||x||'</id>'
              ||'<f_name>W_'|| batchIdx || '</f_name>'
              ||'<l_name>Q_' || x || '</l_name>'
              ||'<GENDER>' || gender || '</GENDER>'
              ||'</emp>';
    insert into xmlagg_table_warehouse_20240726(w_id,w_name,w_spec,w_spec_xml)
    values(x,
      'w'||x,
      xmlString,
      xmltype(xmlString)
    );
  end loop;
end xmlagg_pl_dataseeding_20240726;
/

begin
  xmlagg_pl_dataseeding_20240726(500,0);
end;
/

DECLARE
    xmlagg_res XMLType;
BEGIN
    SELECT XMLAGG(XMLPARSE(CONTENT t.w_id || '<' WELLFORMED) ORDER BY t.w_id) INTO xmlagg_res FROM xmlagg_table_warehouse_20240726 t where w_id < 10;
    DBMS_OUTPUT.PUT_LINE(xmlagg_res.getClobVal());
END;
/

DECLARE
    xmlagg_res XMLType;
BEGIN
    SELECT XMLAGG(XMLPARSE(CONTENT nullif('a','b') WELLFORMED) ORDER BY t.w_id) INTO xmlagg_res FROM xmlagg_table_warehouse_20240726 t where w_id < 10;
    DBMS_OUTPUT.PUT_LINE(xmlagg_res.getClobVal());
END;
/

DECLARE
    xmlagg_res XMLType;
BEGIN
    SELECT XMLAGG(XMLPARSE(DOCUMENT t.W_SPEC WELLFORMED) ORDER BY t.w_id) INTO xmlagg_res FROM xmlagg_table_warehouse_20240726 t where t.w_id <= 2;
    DBMS_OUTPUT.PUT_LINE(xmlagg_res.getClobVal());
END;
/

DECLARE
    xmlagg_res XMLType;
BEGIN
    SELECT XMLAGG(extract(XMLPARSE(DOCUMENT t.W_SPEC WELLFORMED),'//emp/f_name|//emp/l_name') ORDER BY t.w_id) INTO xmlagg_res FROM xmlagg_table_warehouse_20240726 t where t.w_id <= 2;
    DBMS_OUTPUT.PUT_LINE(xmlagg_res.getClobVal());
END;
/

DECLARE
    xmlagg_res XMLType;
BEGIN
    SELECT XMLAGG(XMLPARSE(DOCUMENT t.W_SPEC) ORDER BY t.w_id) INTO xmlagg_res FROM xmlagg_table_warehouse_20240726 t where t.w_id <= 2;
    DBMS_OUTPUT.PUT_LINE(xmlagg_res.getClobVal());
END;
/

drop procedure xmlagg_pl_dataseeding_20240726;
drop table xmlagg_table_warehouse_20240726;
