\c regression_ora
-- tests opentenbase_ora xml opentenbase_ora
create table xmlexample(ID varchar(100),name varchar(20),data opentenbase_ora.xmltype);
insert into xmlexample(id,name,data) values('xxxxxxxxxxxxxxx','my document','<?xml version="1.0" encoding="UTF-8" ?>
<collection xmlns="">
  <record>
    <leader>-----nam0-22-----^^^450-</leader>
    <datafield tag="200" ind1="1" ind2=" ">
      <subfield code="a">抗震救灾</subfield>
       <subfield code="f">奥运会</subfield>
    </datafield>
    <datafield tag="209" ind1=" " ind2=" ">
      <subfield code="a">经济学</subfield>
       <subfield code="b">计算机</subfield>
       <subfield code="c">10001</subfield>
       <subfield code="d">2005-07-09</subfield>
    </datafield>
    <datafield tag="610" ind1="0" ind2=" ">
       <subfield code="a">计算机</subfield>
       <subfield code="a">笔记本</subfield>
    </datafield>
  </record>
</collection>'::opentenbase_ora.xmltype);

select id,name,
      extractvalue(x.data,'/collection/record/leader') as A
from xmlexample x;

select id,name,
         extract(x.data,'/collection/record/datafield/subfield') as A
from xmlexample x;

select id,name,
        extractvalue(x.data,'/collection/record/datafield[@tag="209"]/subfield[@code="a"]') as A
from xmlexample x;

update xmlexample set data = updatexml(data, '/collection/record/leader/text()', 'update_text测试');

select id,name,
      extract(x.data,'/collection/record/leader') as A
from xmlexample x;

select id,name,
      extractvalue(x.data,'/collection/record/leader') as A
from xmlexample x;

drop table xmlexample;

SELECT extract(opentenbase_ora.XMLTYPE('<AAA> <BBB>healthy</BBB> <BBB>sick</BBB> <BBB>other</BBB> </AAA>'),'/AAA/BBB') from dual;

SELECT extract(opentenbase_ora.XMLTYPE('<AAA> <BBB>healthy</BBB> <BBB>sick</BBB> <BBB>other</BBB> </AAA>'),'/AAA/BBB[2]') from dual;

SELECT extractvalue(opentenbase_ora.XMLTYPE('<AAA> <BBB>healthy</BBB> <BBB>sick</BBB> <BBB>other</BBB> </AAA>'),'/AAA/BBB[2]') from dual;

SELECT extractvalue(opentenbase_ora.XMLTYPE('<AAA> <BBB>healthy</BBB> <BBB>sick</BBB> <BBB>other</BBB> </AAA>'),'/AAA/BBB') from dual;

SELECT extract(opentenbase_ora.XMLTYPE('<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope"  xmlns:typ="http://www.gunnarmorling.de/moapa/videorental/types"> <soapenv:Header/>
    <soapenv:Body>
        <typ:FindMoviesByDirectorRequest>
            <Director>Bryan Singer1</Director>
        </typ:FindMoviesByDirectorRequest>
        <typ:FindMoviesByDirectorRequest>
            <Director>Bryan Singer2</Director>
        </typ:FindMoviesByDirectorRequest>
    </soapenv:Body>
</soapenv:Envelope>'),
               '/soapenv:Envelope/soapenv:Body/typ:FindMoviesByDirectorRequest/Director',
               'xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope" xmlns:typ="http://www.gunnarmorling.de/moapa/videorental/types"') from dual;

select extractValue(opentenbase_ora.XMLTYPE('<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope"  xmlns:typ="http://www.gunnarmorling.de/moapa/videorental/types"> <soapenv:Header/>
    <soapenv:Body>
        <typ:FindMoviesByDirectorRequest>
            <Director>Bryan Singer</Director>
        </typ:FindMoviesByDirectorRequest>
    </soapenv:Body>
</soapenv:Envelope>'),
                    '/soapenv:Envelope/soapenv:Body/typ:FindMoviesByDirectorRequest/Director',
                    'xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope" xmlns:typ="http://www.gunnarmorling.de/moapa/videorental/types"') from dual ;

SELECT extractValue(opentenbase_ora.XMLTYPE('<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope"  xmlns:typ="http://www.gunnarmorling.de/moapa/videorental/types"> <soapenv:Header/>
    <soapenv:Body>
        <typ:FindMoviesByDirectorRequest>
            <Director>Bryan Singer1</Director>
        </typ:FindMoviesByDirectorRequest>
        <typ:FindMoviesByDirectorRequest>
            <Director>Bryan Singer2</Director>
        </typ:FindMoviesByDirectorRequest>
    </soapenv:Body>
</soapenv:Envelope>'),
               '/soapenv:Envelope/soapenv:Body/typ:FindMoviesByDirectorRequest/Director',
               'xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope" xmlns:typ="http://www.gunnarmorling.de/moapa/videorental/types"') from dual;

SELECT EXTRACTVALUE(
               opentenbase_ora.XMLTYPE('<ns:root xmlns:ns="http://example.com">'
                   || '<ns:element>Value</ns:element>'
                   || '</ns:root>'),
           '/ns:root/ns:element',
           'xmlns:ns="http://example.com"'
       ) AS extracted_value
FROM dual;

SELECT EXTRACTVALUE(opentenbase_ora.XMLTYPE('<ns:root xmlns:ns="http://example.com"><ns:element>Value</ns:element></ns:root>'), '/ns:root/ns:element', 'xmlns:ns="http://example.com"') FROM dual;

SELECT EXTRACTVALUE(opentenbase_ora.xmltype('<ns1:books xmlns:ns1="http://example.com/books" xmlns:ns2="http://example.com/authors">
  <ns1:book>
    <ns1:title>opentenbase_ora Database 12c SQL Fundamentals</ns1:title>
    <ns2:author>John Smith</ns2:author>
    <ns1:year>2019</ns1:year>
  </ns1:book>
  <ns1:book>
    <ns1:title>Java Programming 101</ns1:title>
    <ns2:author>Jane Doe</ns2:author>
    <ns1:year>2020</ns1:year>
  </ns1:book>
</ns1:books>'), '/ns1:books/ns1:book[1]/ns1:title', 'xmlns:ns1="http://example.com/books" xmlns:ns2="http://example.com/authors"') AS title,
EXTRACTVALUE(opentenbase_ora.xmltype('<ns1:books xmlns:ns1="http://example.com/books" xmlns:ns2="http://example.com/authors">
  <ns1:book>
    <ns1:title>opentenbase_ora Database 12c SQL Fundamentals</ns1:title>
    <ns2:author>John Smith</ns2:author>
    <ns1:year>2019</ns1:year>
  </ns1:book>
  <ns1:book>
    <ns1:title>Java Programming 101</ns1:title>
    <ns2:author>Jane Doe</ns2:author>
    <ns1:year>2020</ns1:year>
  </ns1:book>
</ns1:books>'), '/ns1:books/ns1:book[2]/ns2:author', 'xmlns:ns1="http://example.com/books" xmlns:ns2="http://example.com/authors"') AS author
FROM dual;

CREATE TABLE xmltest (
                         id int,
                         data opentenbase_ora.xmltype
);

INSERT INTO xmltest VALUES (1, '<value>one</value>');
INSERT INTO xmltest VALUES (2, '<value>two</value>');
INSERT INTO xmltest VALUES (3, '<wrong');

insert into xmltest values(200, opentenbase_ora.XMLType('<?xml version="1.0" encoding="UTF-8" ?>
<collection xmlns="">
  <record>
    <leader>-----nam0-22-----^^^450-</leader>
    <datafield tag="200" ind1="1" ind2=" ">
      <subfield code="a">抗震救灾</subfield>
       <subfield code="f">奥运会</subfield>
    </datafield>
    <datafield tag="209" ind1=" " ind2=" ">
      <subfield code="a">经济学</subfield>
       <subfield code="b">计算机</subfield>
       <subfield code="c">10001</subfield>
       <subfield code="d">2005-07-09</subfield>
    </datafield>
    <datafield tag="610" ind1="0" ind2=" ">
       <subfield code="a">计算机</subfield>
       <subfield code="b">笔记本</subfield>
    </datafield>
  </record>
</collection>'));
insert into xmltest values(400, null);
UPDATE xmltest SET data = updatexml(data,
      '/collection/record/datafield[@tag="209"]/subfield[@code="a"]/text()','English',
      '/collection/record/datafield[@tag="610"]/subfield[@code="a"]/text()','Computer',
      '//datafield[@tag="200"]',opentenbase_ora.xmltype(
              '<datafield tag="200" ind1="1" ind2=" "></datafield>'));
insert into xmltest values(300, opentenbase_ora.xmltype('<EMPLOYEES>
   <EMP>
    <EMPNO>112</EMPNO>
    <EMPNAME>Joe</EMPNAME>
    <SALARY>100000</SALARY>
  </EMP>
  <EMP>
    <EMPNO>217</EMPNO>
    <EMPNAME>Jane</EMPNAME>
   </EMP>
  <EMP>
    <EMPNO>412</EMPNO>
    <EMPNAME>Jackson</EMPNAME>
    <SALARY>40000</SALARY>
  </EMP>
</EMPLOYEES>'));
drop view if exists test_xmltype_v;
CREATE VIEW test_xmltype_v
   AS SELECT
      UPDATEXML(data, '/EMPLOYEES/EMP/SALARY/text()', 0) emp_view_col
   FROM xmltest where id=300;
select * from test_xmltype_v;
update xmltest set data = updatexml(data,'//EMP[EMPNO=217]',NULL) where id = 300;
update xmltest set data = updatexml(data,'//EMP[EMPNO=412]/EMPNAME/text()',NULL) where id = 300;
select * from xmltest order by 1,2;

INSERT INTO xmltest VALUES (3, 'wrong');

SELECT extract(data, '/value') FROM xmltest ORDER BY id;

SELECT extract(data, null) FROM xmltest ORDER BY id;

SELECT extract(null, '/value') FROM xmltest ORDER BY id;

SELECT extract(opentenbase_ora.XMLTYPE('<!-- error -->'), '') from dual;

SELECT extract(opentenbase_ora.XMLTYPE('<local:data xmlns:local="http://127.0.0.1"><local:piece id="1">number one</local:piece><local:piece id="2" /></local:data>'), '//text()') from dual;

drop table xmltest cascade;

drop table if exists test_XMLType;
create table test_XMLType(f1 int,f2 opentenbase_ora.XMLType) distribute By SHARD(f1);
insert into test_XMLType values(1,'<ROW>XMLType2</ROW>');
insert into test_XMLType values(2,'<ROW>XMLType2</ROW>'),(3,'<ROW>XMLType2</ROW>');
select distinct f2 from test_XMLType;
drop table test_XMLType;
