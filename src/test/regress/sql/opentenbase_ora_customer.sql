\c regression_ora
set enable_pullup_subquery TO on;

DROP TABLE IF EXISTS ch_pw_etinspectpic;
CREATE TABLE ch_pw_etinspectpic (
    itemid numeric(14,0) NOT NULL,
    code character varying(128) NOT NULL,
    picname character varying(128) NOT NULL,
    updatedate timestamp(0) without time zone NOT NULL,
    state numeric(3,0) NOT NULL
)
DISTRIBUTE BY SHARD (itemid);

DROP TABLE IF EXISTS sa_db_dictitem;
CREATE TABLE sa_db_dictitem (
    dictid character varying(32) NOT NULL,
    groupid character varying(32) NOT NULL,
    region numeric(5,0) NOT NULL,
    dictname character varying(64),
    sortorder numeric(3,0),
    status numeric(1,0),
    statusdate timestamp(0) without time zone,
    description character varying(128),
    parentdictid character varying(128)
)
DISTRIBUTE BY SHARD (dictid);

DROP TABLE IF EXISTS ch_pw_etinspectitemstd;
CREATE TABLE ch_pw_etinspectitemstd (
    version character varying(18) NOT NULL,
    itemid numeric(14,0) NOT NULL,
    inspecttype character varying(24) NOT NULL,
    levtwostd character varying(24) NOT NULL,
    levthrstd character varying(64) NOT NULL,
    content character varying(512) NOT NULL,
    itemadmim character varying(32) NOT NULL,
    createdate timestamp(0) without time zone NOT NULL,
    inspectmode character varying(4) NOT NULL,
    inspectobj character varying(6) NOT NULL,
    updatedetail numeric(3,0),
    score numeric(5,0) NOT NULL,
    region numeric(5,0) NOT NULL,
    approver character varying(32)
)
DISTRIBUTE BY SHARD (version);

DROP TABLE IF EXISTS ch_pw_etinspectstd;
CREATE TABLE ch_pw_etinspectstd (
    version character varying(18) NOT NULL,
    name character varying(100) NOT NULL,
    startdate timestamp(0) without time zone NOT NULL,
    enddate timestamp(0) without time zone NOT NULL,
    pubdate timestamp(0) without time zone NOT NULL,
    publisher character varying(32) NOT NULL,
    region numeric(5,0) NOT NULL,
    remark character varying(512),
    state numeric(3,0) NOT NULL
)
DISTRIBUTE BY SHARD (version);

DROP TABLE IF EXISTS ch_pw_etinspectrcd;
CREATE TABLE ch_pw_etinspectrcd (
    version character varying(18) NOT NULL,
    itemid numeric(14,0) NOT NULL,
    wayid character varying(18) NOT NULL,
    result numeric(3,0) DEFAULT 1 NOT NULL,
    sulotion character varying(512),
    sulotiondate timestamp(0) without time zone,
    followupperson character varying(32),
    recordperson character varying(32) NOT NULL,
    approver character varying(32),
    followucall character varying(20),
    partner character varying(100),
    recordcontent character varying(400),
    recorddate timestamp(0) without time zone NOT NULL,
    approvedate timestamp(0) without time zone,
    remark character varying(512),
    endremark character varying(512),
    state numeric(3,0) NOT NULL
)
DISTRIBUTE BY SHARD (version);

DROP TABLE IF EXISTS ch_td_attendanceresult;
CREATE TABLE ch_td_attendanceresult (
    staffid character varying(32) NOT NULL,
    attendancedate timestamp(0) without time zone NOT NULL,
    ambeginresult character varying(16) NOT NULL,
    amendresult character varying(16) NOT NULL,
    pmbeginresult character varying(16) NOT NULL,
    pmendresult character varying(16) NOT NULL,
    ambeginresultoid numeric(14,0),
    amendresultoid numeric(14,0),
    pmbeginresultoid numeric(14,0),
    pmendresultoid numeric(14,0)
)
DISTRIBUTE BY SHARD (staffid);

DROP TABLE IF EXISTS ch_ngsettle_subdetail;
CREATE TABLE ch_ngsettle_subdetail (
    region numeric(5,0) NOT NULL,
    cycle numeric(8,0) NOT NULL,
    oid character varying(32) NOT NULL,
    totaloid character varying(32),
    relativeoid character varying(32),
    recoid character varying(32),
    occurcycle numeric(8,0),
    packageid character varying(32) NOT NULL,
    instanceoid numeric(14,0) NOT NULL,
    rewardid character varying(32) NOT NULL,
    rateoid numeric(14,0),
    usetype character varying(32),
    computetype character varying(32),
    objecttype character varying(32) NOT NULL,
    objectid character varying(32) NOT NULL,
    entitytype character varying(32),
    entityoid character varying(32),
    paycount numeric(2,0),
    resourceid character varying(32),
    statval numeric(16,0),
    rewardval numeric(16,0) NOT NULL,
    paytype character varying(32),
    payquantity numeric(16,0),
    ispay numeric(1,0),
    processdate timestamp(0) without time zone,
    status character varying(32),
    statusdate timestamp(0) without time zone,
    servnumber character varying(25),
    productid character varying(32),
    cycletype character varying(16),
    expr_class character(1),
    res_type_id character varying(32),
    privid character varying(32),
    recopid character varying(32),
    subsid numeric(18,0),
    settletype character varying(32),
    relateobjectid character varying(32),
    relateobjecttype character varying(32),
    carryingtype character varying(32),
    recdefid character varying(32),
    recdate timestamp(0) without time zone,
    startdate timestamp(0) without time zone,
    contacttype character varying(16),
    recorgid character varying(32),
    financeid character varying(32),
    createtype character varying(32),
    topclasstype character varying(32),
    memo character varying(1024),
    countyid character varying(32),
    hropid character varying(32),
    stationid character varying(32),
    rewardtype character varying(32),
    detaildesp character varying(4000),
    imei character varying(20),
    oldprodid character varying(32),
    oldprodfee numeric(16,0),
    opnid character varying(18),
    periods numeric(2,0),
    ratio numeric(16,2),
    intervalmonth numeric(2,0),
    prodfee numeric(16,0),
    formnum character varying(64),
    productbegindate timestamp(0) without time zone,
    brand character varying(16),
    imei2 character varying(20),
    servnumber2 character varying(25),
    texe2 character varying(64),
    texe1 character varying(64),
    adjustkind numeric(2,0),
    imeitype numeric(2,0),
    sceveal numeric(16,2),
    groupoid numeric(14,0),
    groupsubsid character varying(18),
    regservnum character varying(25),
    privproductid character varying(32),
    flag numeric(4,0),
    opnid2 character varying(18),
    srcperiods numeric(2,0),
    cause character varying(256),
    subject character varying(4),
    mobiledata numeric(16,0),
    arpuval numeric(16,0),
    hostcallday numeric(2,0),
    description character varying(256),
    assessscore numeric(8,0),
    waytype character varying(8),
    waysubtype character varying(8),
    origcycle numeric(8,0),
    version numeric(14,0),
    calctype numeric(1,0),
    lastversion numeric(14,0),
    lastrewardval numeric(16,0),
    diffrewardval numeric(16,0)
)
DISTRIBUTE BY SHARD (region);

DROP TABLE IF EXISTS agent;
CREATE TABLE agent (
    cycle numeric(8,0),
    region numeric(5,0) NOT NULL,
    agentid character varying(32),
    agenttype character varying(16),
    agentlevel character varying(16),
    starlevel character varying(16),
    areastyle character varying(32),
    cooperatetype character varying(32),
    customtype character varying(32),
    entity_oidn numeric(14,0),
    istax numeric(2,0),
    countyid character varying(32),
    agentstyle character varying(32),
    deadline timestamp(0) without time zone,
    waytype character varying(16),
    exclusiveness numeric(2,0),
    waystate numeric(2,0),
    partnertype character varying(2),
    sourcecode character varying(64),
    sourcename character varying(512),
    distdevmodulus character varying(8),
    microdistdevmodulus character varying(8),
    chaincooperid character varying(64),
    distid character varying(20),
    distname character varying(64),
    microdistid character varying(20),
    servtype numeric(2,0),
    waysubtype character varying(8),
    memo character varying(256),
    statedate timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (region);

DROP TABLE IF EXISTS ch_way_societyinfo;
CREATE TABLE ch_way_societyinfo (
    uniformcode character varying(64),
    upperwayid character varying(64),
    upperwayname character varying(256),
    societywayid character varying(64) NOT NULL,
    societywayname character varying(512),
    region numeric(5,0) NOT NULL,
    waystate numeric(2,0),
    waytype character varying(4),
    waysubtype character varying(4),
    waysystem numeric(3,0),
    waylevel numeric(3,0),
    starlevel numeric(3,0),
    cityid character varying(64) NOT NULL,
    countyid character varying(64),
    svccode character varying(64),
    mareacode character varying(64),
    modifydate timestamp(0) without time zone,
    kind numeric(2,0),
    custtype character varying(4),
    isnew numeric(2,0),
    buztypecode numeric(2,0) DEFAULT 3,
    adtypecode numeric(2,0) DEFAULT 0,
    isbbchannel numeric(2,0) DEFAULT 0 NOT NULL,
    bbefftime timestamp(0) without time zone,
    bbfailtime timestamp(0) without time zone,
    bbcoopfile character varying(16),
    manageroprcode character varying(16),
    signstate numeric(2,0) DEFAULT 2 NOT NULL,
    paycontractid character varying(64),
    islockrwaccount numeric(2,0) DEFAULT 0 NOT NULL,
    createdate timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (region);

DROP TABLE IF EXISTS ch_bl_waycoefficient;
CREATE TABLE ch_bl_waycoefficient (
    region numeric(5,0) NOT NULL,
    cycle numeric(8,0) NOT NULL,
    type numeric(3,0) NOT NULL,
    wayid character varying(18) NOT NULL,
    coefficient numeric(18,4) NOT NULL
)
DISTRIBUTE BY SHARD (region);

DROP TABLE IF EXISTS rd_st_subdetail;
CREATE TABLE rd_st_subdetail (
    oid numeric(14,0) NOT NULL,
    cycle numeric(8,0) NOT NULL,
    region numeric(5,0) NOT NULL,
    orderid character varying(64),
    orgcycle numeric(8,0) NOT NULL,
    policyid character varying(18),
    ruleid character varying(18),
    flowid character varying(18),
    entitytype character varying(32) NOT NULL,
    entityoid character varying(256),
    servnumber character varying(32),
    productid character varying(32),
    recorgid character varying(32),
    recdate timestamp(0) without time zone,
    recopid character varying(32),
    recoid numeric(18,0),
    totalperiods numeric(4,0) NOT NULL,
    currentperiod numeric(4,0) NOT NULL,
    rewardval numeric(16,0) NOT NULL,
    cycletype character varying(16),
    processdate timestamp(0) without time zone NOT NULL,
    memo character varying(4000),
    rewardactval numeric(16,0) DEFAULT 0 NOT NULL,
    calcsrcid character varying(64),
    status numeric(2,0),
    auditfailmemo character varying(4000),
    waytype character varying(8),
    rewardtype character varying(16) DEFAULT 'cmNormal'::character varying NOT NULL
)
DISTRIBUTE BY SHARD (region);

DROP TABLE IF EXISTS rd_st_failurelog;
CREATE TABLE rd_st_failurelog (
    oid numeric(14,0) NOT NULL,
    cycle numeric(8,0) NOT NULL,
    region numeric(5,0) NOT NULL,
    orderid character varying(64),
    orgcycle numeric(8,0) NOT NULL,
    policyid character varying(18),
    ruleid character varying(18),
    flowid character varying(18),
    entitytype character varying(32) NOT NULL,
    entityoid character varying(256),
    servnumber character varying(32),
    productid character varying(32),
    recorgid character varying(32),
    recdate timestamp(0) without time zone,
    recopid character varying(32),
    recoid numeric(18,0),
    totalperiods numeric(4,0) NOT NULL,
    currentperiod numeric(4,0) NOT NULL,
    cycletype character varying(16),
    processdate timestamp(0) without time zone NOT NULL,
    memo character varying(4000),
    description character varying(1024),
    factorid numeric(8,0),
    rewardval numeric(16,0) DEFAULT 0 NOT NULL,
    rewardactval numeric(16,0) DEFAULT 0 NOT NULL,
    calcsrcid character varying(64),
    waytype character varying(8),
    rewardtype character varying(16) DEFAULT 'cmNormal'::character varying NOT NULL
)
DISTRIBUTE BY SHARD (region);

DROP TABLE IF EXISTS rd_st_rewardprocrec;
CREATE TABLE rd_st_rewardprocrec (
    cycle numeric(8,0) NOT NULL,
    region numeric(5,0) NOT NULL,
    taskid numeric(14,0) NOT NULL,
    process character varying(32) NOT NULL,
    batchlist character varying(128),
    starttime timestamp(0) without time zone,
    endtime timestamp(0) without time zone,
    progress numeric(5,2),
    status numeric(2,0) NOT NULL,
    statustime timestamp(0) without time zone,
    memo character varying(4000),
    ordercnt numeric(14,0),
    orderdatasrc character varying(32),
    serverinfo character varying(128)
)
DISTRIBUTE BY SHARD (region);

DROP TABLE IF EXISTS rd_st_recalcapplydetail;
CREATE TABLE rd_st_recalcapplydetail (
    applyoid numeric(14,0) NOT NULL,
    classifyid character varying(32) NOT NULL,
    taskcode character varying(64) NOT NULL
)
DISTRIBUTE BY SHARD (applyoid);

DROP TABLE IF EXISTS rd_cf_ruledef;
CREATE TABLE rd_cf_ruledef (
    ruleid character varying(18) NOT NULL,
    rulename character varying(256) NOT NULL,
    createoperid character varying(32),
    beloncommsid numeric(8,0),
    ruledesc character varying(1024),
    comclsify character varying(16),
    comlcls character varying(16),
    comscls character varying(16),
    calctype character varying(32),
    region numeric(5,0) NOT NULL,
    begindate timestamp(0) without time zone,
    enddate timestamp(0) without time zone,
    createtime timestamp(0) without time zone,
    cycle numeric(8,0) NOT NULL,
    attachid numeric(16,0),
    bomcid character varying(32),
    ruleextends character varying(32),
    subtypecode character varying(16),
    status numeric(3,0) DEFAULT 1 NOT NULL,
    statustime timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (region);

DROP TABLE IF EXISTS rd_cf_policydef;
CREATE TABLE rd_cf_policydef (
    policyid numeric(8,0) NOT NULL,
    policyname character varying(256) NOT NULL,
    draftoperid character varying(32),
    drafttime timestamp(0) without time zone,
    text text,
    status numeric(3,0),
    region numeric(5,0) NOT NULL,
    attachid numeric(16,0),
    bomcid character varying(32)
)
DISTRIBUTE BY SHARD (region);

DROP TABLE IF EXISTS rd_cf_ruleflow;
CREATE TABLE rd_cf_ruleflow (
    ruleflowid numeric(14,0) NOT NULL,
    ruleid character varying(18) NOT NULL,
    cycle numeric(8,0) NOT NULL,
    createoperid character varying(32),
    createtime timestamp(0) without time zone,
    status numeric(3,0),
    begindate timestamp(0) without time zone,
    enddate timestamp(0) without time zone,
    region numeric(5,0) NOT NULL,
    rulemode character varying(16) DEFAULT 'StandFlowChat'::character varying NOT NULL,
    stdmode character varying(16) DEFAULT 'RATIOVALUE'::character varying NOT NULL,
    channelfile numeric(16,0),
    auditid numeric(14,0),
    rulefittype character varying(8) DEFAULT 3,
    ruletype character varying(32) DEFAULT 'OneTimeCalc'::character varying NOT NULL,
    bomcid character varying(32),
    attachid numeric(16,0)
)
DISTRIBUTE BY SHARD (region);

DROP TABLE IF EXISTS rd_cf_flowdef;
CREATE TABLE rd_cf_flowdef (
    ruleflowid numeric(14,0) NOT NULL,
    flowid numeric(14,0) NOT NULL,
    createoperid character varying(32),
    createtime timestamp(0) without time zone,
    updatetime timestamp(0) without time zone,
    cycle numeric(8,0) NOT NULL,
    flowtype character varying(15) NOT NULL
)
DISTRIBUTE BY SHARD (ruleflowid);

DROP TABLE IF EXISTS sa_sys_operator;
CREATE TABLE sa_sys_operator (
    operid character varying(32) NOT NULL,
    opercode character varying(16),
    opername character varying(64) NOT NULL,
    nickname character varying(64),
    status character varying(2) NOT NULL,
    statusdate timestamp(0) without time zone NOT NULL,
    orgid character varying(64) NOT NULL,
    region numeric(5,0),
    staffid character varying(16) NOT NULL,
    stafftype numeric(1,0),
    postid character varying(20) NOT NULL,
    idtype character varying(16),
    contactphone character varying(20),
    idcard character varying(128),
    starttime timestamp(0) without time zone,
    endtime timestamp(0) without time zone,
    createdate timestamp(0) without time zone NOT NULL,
    notes character varying(256),
    validatemode numeric(1,0),
    macaddress character varying(32),
    ipaddress character varying(32),
    password character varying(128) NOT NULL,
    pwdchgdate timestamp(0) without time zone,
    pwdenddate timestamp(0) without time zone NOT NULL,
    isneedchpwd numeric(2,0),
    portaluserid character varying(32),
    passwordmdfive character varying(128),
    fouraccount character varying(32),
    upperoperid character varying(32)
)
DISTRIBUTE BY SHARD (operid);

DROP TABLE IF EXISTS sa_sys_post;
CREATE TABLE sa_sys_post (
    postid character varying(20) NOT NULL,
    postname character varying(64) NOT NULL,
    postattach character varying(8),
    posttype character varying(8) NOT NULL,
    region numeric(5,0),
    status numeric(1,0) NOT NULL,
    statusdate timestamp(0) without time zone NOT NULL,
    createdate timestamp(0) without time zone NOT NULL,
    updatedate timestamp(0) without time zone,
    notes character varying(256),
    iswayonly character varying(8) DEFAULT 0 NOT NULL
)
DISTRIBUTE BY SHARD (postid);

DROP TABLE IF EXISTS sa_sys_staffinfo;
CREATE TABLE sa_sys_staffinfo (
    staffid character varying(16) NOT NULL,
    name character varying(64) NOT NULL,
    sex numeric(2,0),
    nation character varying(64),
    idcard character varying(128) NOT NULL,
    age character varying(16),
    state numeric(2,0),
    statedci character varying(256),
    phonenumber character varying(16),
    busphonenumber character varying(16) NOT NULL,
    marriedstate numeric(2,0),
    childbirdate timestamp(0) without time zone,
    town character varying(256),
    homeaddress character varying(256),
    realaddr character varying(256),
    hascar numeric(2,0),
    joinworktime timestamp(0) without time zone,
    joinmobiletime timestamp(0) without time zone,
    mobileseniority character varying(16),
    politicalnature character varying(32),
    householdregaddr character varying(256),
    nativeplace character varying(256),
    birthaddr character varying(256),
    householdtype numeric(2,0),
    householeaddr character varying(256),
    birthday timestamp(0) without time zone,
    photo character varying(512),
    email character varying(64),
    isrelwork numeric(2,0),
    memo character varying(512),
    employeenum character varying(32),
    portalid character varying(32),
    bossid character varying(32),
    stafftype numeric(2,0),
    coopcompany character varying(64),
    wayname character varying(256),
    wayid character varying(16),
    cityid character varying(16),
    countyid character varying(16),
    township character varying(32),
    graduatedschool character varying(64),
    major character varying(64),
    academicbackground character varying(16),
    degree character varying(32),
    learnmajor character varying(64),
    learntype character varying(64),
    specialty character varying(64),
    jobname character varying(32),
    backgroundterm character varying(32),
    positionname character varying(32),
    positionclassify character varying(32),
    positionseq character varying(32),
    admlevel character varying(32),
    positionrank character varying(32),
    paymentclassify character varying(16),
    grouppositionname character varying(32),
    gdpositionname character varying(32),
    isunionmember numeric(2,0),
    realjobname character varying(32),
    ispiecerate numeric(2,0),
    assesspost character varying(32),
    unassessreason character varying(512),
    postauth character varying(32),
    performance character varying(512),
    abilitypoints character varying(32),
    jobpoints character varying(32),
    satisfaction character varying(32),
    honorinfo character varying(1024),
    disciplineinfo character varying(1024),
    growthmark character varying(1024),
    auditstate character varying(16),
    opeatorname character varying(16),
    modifydate timestamp(0) without time zone,
    sysefftime timestamp(0) without time zone,
    sysfailtime timestamp(0) without time zone,
    isblacklist numeric(2,0),
    pullblackreason character varying(512),
    stafftag character varying(64),
    createdate timestamp(0) without time zone,
    assesstype numeric(2,0)
)
DISTRIBUTE BY SHARD (staffid);

DROP TABLE IF EXISTS ch_td_workingschedual;
CREATE TABLE ch_td_workingschedual (
    operid character varying(32) NOT NULL,
    staffid character varying(32) NOT NULL,
    workdate numeric(8,0) NOT NULL,
    amworktype character varying(64),
    amworkcontent character varying(64),
    ambegintime character varying(64),
    amendtime character varying(64),
    pmworktype character varying(64),
    pmworkcontent character varying(64),
    pmbegintime character varying(64),
    pmendtime character varying(64),
    state numeric(2,0) NOT NULL,
    pmstate numeric(2,0) DEFAULT 1 NOT NULL
)
DISTRIBUTE BY SHARD (operid);

DROP TABLE IF EXISTS ch_td_worktype;
CREATE TABLE ch_td_worktype (
    worktypeid numeric(6,0) NOT NULL,
    bworktype numeric(2,0) NOT NULL,
    worktypename character varying(64) NOT NULL,
    state numeric(1,0) NOT NULL,
    region numeric(5,0) NOT NULL,
    isneedpunch numeric(1,0) NOT NULL
)
DISTRIBUTE BY SHARD (worktypeid);

DROP TABLE IF EXISTS ch_td_taskdetail;
CREATE TABLE ch_td_taskdetail (
    taskid numeric(14,0) NOT NULL,
    infoid numeric(14,0) NOT NULL,
    receiver character varying(32) NOT NULL,
    dispatchedby character varying(32),
    parenttaskid numeric(14,0),
    tasktype numeric(1,0) NOT NULL,
    state numeric(1,0) NOT NULL,
    startdate timestamp(0) without time zone NOT NULL,
    enddate timestamp(0) without time zone NOT NULL,
    ismainreceiver numeric(1,0) NOT NULL,
    finisheddate timestamp(0) without time zone,
    wayid character varying(32),
    isneedfeedback numeric(1,0) NOT NULL,
    issubtask numeric(1,0) NOT NULL,
    iscollection numeric(1,0) NOT NULL,
    ismultiway numeric(1,0) NOT NULL,
    isread numeric(1,0) NOT NULL,
    updatedate timestamp(0) without time zone,
    istoprecord numeric(1,0) DEFAULT 0 NOT NULL,
    readdate timestamp(0) without time zone,
    ischecked numeric(1,0) DEFAULT 0 NOT NULL,
    carryhours character varying(16),
    approvallevel character varying(3),
    confirm numeric(1,0),
    region numeric(5,0)
)
DISTRIBUTE BY SHARD (taskid);

DROP TABLE IF EXISTS ch_td_inforeceiverrela;
CREATE TABLE ch_td_inforeceiverrela (
    infoid numeric(14,0) NOT NULL,
    receiver character varying(32) NOT NULL,
    createdate timestamp(0) without time zone NOT NULL,
    isread numeric(1,0) NOT NULL,
    iscollection numeric(1,0) NOT NULL,
    readdate timestamp(0) without time zone,
    readtool numeric(1,0),
    updatedate timestamp(0) without time zone,
    istoprecord numeric(1,0) DEFAULT 0 NOT NULL
)
DISTRIBUTE BY SHARD (infoid);

DROP TABLE IF EXISTS ch_td_trainingshare;
CREATE TABLE ch_td_trainingshare (
    shareid numeric(14,0) NOT NULL,
    content character varying(2048) NOT NULL,
    sharedate timestamp(0) without time zone NOT NULL,
    shareby character varying(32) NOT NULL,
    wayid character varying(32) NOT NULL,
    traintype character varying(64) NOT NULL,
    traindate timestamp(0) without time zone NOT NULL,
    infoid numeric(14,0)
)
DISTRIBUTE BY SHARD (shareid);

DROP TABLE IF EXISTS t_bme_publicdatadict;
CREATE TABLE t_bme_publicdatadict (
    typeid character varying(100) NOT NULL,
    typedesc character varying(72),
    dataid character varying(255) NOT NULL,
    datadesc character varying(255),
    cityid character varying(8) NOT NULL,
    version character varying(4) NOT NULL,
    status character varying(24),
    effdate timestamp(0) without time zone,
    expdate timestamp(0) without time zone,
    orderno numeric DEFAULT 0,
    lastupdateperson character varying(32),
    lastupdatedate timestamp(0) without time zone,
    notes character varying(255),
    column1 character varying(255),
    column2 character varying(255),
    column3 character varying(255),
    column4 character varying(255),
    column5 character varying(255),
    column6 character varying(255),
    column7 character varying(255),
    column8 character varying(255),
    column9 character varying(255),
    column10 character varying(255)
)
DISTRIBUTE BY SHARD (cityid);

DROP TABLE IF EXISTS t_bme_languagelocaldisplay;
CREATE TABLE t_bme_languagelocaldisplay (
    keyindex character varying(255) NOT NULL,
    localeindex character varying(12) NOT NULL,
    msginfo character varying(255)
)
DISTRIBUTE BY SHARD (keyindex);

DROP TABLE IF EXISTS sa_sys_operlog;
CREATE TABLE sa_sys_operlog (
    logid numeric(14,0) NOT NULL,
    menuid character varying(64) NOT NULL,
    menulevel numeric(1,0) NOT NULL,
    parentid character varying(64) NOT NULL,
    operid character varying(20) NOT NULL,
    opertype numeric(1,0) NOT NULL,
    operdesc numeric(1,0),
    opertime timestamp(0) without time zone NOT NULL,
    sysin character varying(20) NOT NULL
)
DISTRIBUTE BY SHARD (logid);

DROP TABLE IF EXISTS sa_sys_menu;
CREATE TABLE sa_sys_menu (
    menuid character varying(64) NOT NULL,
    name character varying(128) NOT NULL,
    action character varying(255),
    isleaf numeric,
    menulevel numeric,
    parentid character varying(64) NOT NULL,
    menuorder numeric,
    issystem numeric,
    ishttps numeric,
    status numeric,
    createoperid character varying(64),
    createdate timestamp(6) without time zone,
    updateoperid character varying(64),
    updatedate timestamp(6) without time zone,
    isshow numeric DEFAULT 1 NOT NULL,
    systemcode character varying(24)
)
DISTRIBUTE BY SHARD (menuid);

DROP TABLE IF EXISTS ch_ccm_societyinfo;
CREATE TABLE ch_ccm_societyinfo (
    deduaccounttype numeric(2,0) NOT NULL,
    createdate timestamp(0) without time zone,
    recid character varying(16) NOT NULL,
    societywayid character varying(64),
    storeid character varying(64),
    exclusiveness numeric(2,0),
    subsidyapply character varying(16),
    maname character varying(64),
    buzarea numeric(10,2),
    dredetail character varying(512),
    chaincooperator character varying(64),
    buztypeid character varying(64),
    chaincooperid character varying(64),
    buztypename character varying(64),
    buztypelevel character varying(64),
    waymagcode character varying(64),
    stokeeper character varying(64),
    stokeeperno character varying(128),
    isrelwork character varying(2),
    stokemail character varying(64),
    responname character varying(64),
    registrationno character varying(256),
    lisencename character varying(64),
    lisencefile character varying(16),
    pospicture character varying(16),
    opppicture character varying(16),
    leftpicture character varying(16),
    rigpicture character varying(16),
    inspicture character varying(16),
    sysefftime timestamp(0) without time zone,
    logisticsid character varying(64),
    recename character varying(64),
    recenumber character varying(128),
    rececerid character varying(128),
    sendaddr character varying(256),
    contractname character varying(256),
    contractcode character varying(64),
    firconefftime timestamp(0) without time zone,
    currconefftime timestamp(0) without time zone,
    currconfailtime timestamp(0) without time zone,
    maintemargin numeric(10,2),
    mainteamount numeric(10,2),
    maintebill character varying(16),
    taxcert numeric(2,0),
    taxsignid character varying(64),
    taxpregisnum character varying(64),
    taxregisfile character varying(16),
    leasecontract character varying(16),
    busiauthor character varying(16),
    integagreement character varying(16),
    supplemagreement character varying(16),
    otheragreement character varying(16),
    contracttext character varying(16),
    rempaybankname character varying(64),
    rempaybankaccount character varying(128),
    rempaybankid character varying(128),
    rempayperson character varying(64),
    taxrate numeric(3,1),
    rempayaccountid character varying(128),
    deduofbankname character varying(64),
    deduofbankaccount character varying(128),
    deduofopenaccbank character varying(64),
    lastupdatetime timestamp(0) without time zone,
    currconeffnote character varying(256),
    waymagname character varying(64),
    rempayaccounttype numeric(2,0),
    isparticipatededu character varying(4),
    wayquittime timestamp(0) without time zone,
    wayauditresult character varying(16),
    prestatefile character varying(16),
    datamodifyfile character varying(16),
    quitstatefile character varying(16),
    waynote character varying(256),
    creditlevel numeric(2,0),
    responnumber character varying(128),
    responidimage character varying(16),
    deduofaccname character varying(64),
    sysfailtime timestamp(0) without time zone,
    apprpastime timestamp(0) without time zone,
    logisticsname character varying(64),
    countycompname character varying(64),
    svcname character varying(64),
    layer character varying(32),
    blockcode character varying(64),
    yskaccountbankname character varying(256),
    yskaccountbank character varying(256),
    yskaccountnum character varying(256),
    yskaccountname character varying(256),
    yskaccounttype character varying(5),
    yskaccountidcardnum character varying(256),
    partnerrela character varying(256),
    soucecode character varying(256),
    channelcoefficient character varying(16),
    onetimesubsidy numeric(10,2),
    intoprtype character varying(10),
    serialid character varying(32),
    sourceplatform character varying(32),
    distid character varying(20),
    microdistid character varying(20),
    partnertype character varying(2) DEFAULT '3'::character varying,
    sourcecode character varying(64),
    sourcename character varying(512),
    deduofcertid character varying(128),
    consigntime timestamp(0) without time zone,
    ispolysourc numeric(2,0),
    creditlevelfile character varying(16),
    coreareaid character varying(20),
    salebucklebankmobile character varying(20),
    manageroprcode character varying(32),
    busiexpiredate timestamp(0) without time zone,
    storefronttype numeric(2,0) DEFAULT 0 NOT NULL,
    sitescore numeric(4,2),
    managetype numeric(2,0) DEFAULT 0 NOT NULL,
    storearea numeric(10,2) DEFAULT 0.1 NOT NULL,
    longitude character varying(20),
    latitude character varying(20)
)
DISTRIBUTE BY SHARD (deduaccounttype);

DROP TABLE IF EXISTS ch_way_district;
CREATE TABLE ch_way_district (
    distid character varying(20) NOT NULL,
    distname character varying(64),
    cityid character varying(20),
    countyid character varying(20),
    distdescribe character varying(512),
    distmap character varying(256),
    memo character varying(512),
    createdate timestamp(0) without time zone,
    createoperid character varying(32),
    updatedate timestamp(0) without time zone,
    state numeric(3,0),
    regiontype numeric(3,0) DEFAULT 0 NOT NULL,
    distproperty numeric(3,0) DEFAULT 6 NOT NULL,
    distgrade numeric(3,0) DEFAULT 0 NOT NULL,
    distdevmodulus numeric(10,2)
)
DISTRIBUTE BY SHARD (distid);

DROP TABLE IF EXISTS ch_ccm_franchise;
CREATE TABLE ch_ccm_franchise (
    managetype character varying(4) NOT NULL,
    buztypename character varying(64),
    buztypeid character varying(64),
    subsidyapply character varying(64),
    viversion character varying(64),
    doorlength numeric(6,2),
    doorwidth numeric(6,2),
    lagginglength numeric(6,2),
    laggingwidth numeric(6,2),
    wayid character varying(64),
    panoramicimg character varying(256),
    structureimg character varying(256),
    laggingpic character varying(256),
    doorimg character varying(256),
    construcimg character varying(256),
    firstfixtime timestamp(0) without time zone,
    firstfixamount numeric(10,2),
    firfixinvestid character varying(64),
    firfurniturecost numeric(10,2),
    firvicost numeric(10,2),
    recfixtime timestamp(0) without time zone,
    recfixtype character varying(4),
    recfixamount numeric(10,2),
    recfixinvestid character varying(64),
    recfurniturecost numeric(10,2),
    recvicost numeric(10,2),
    buildingarea character varying(64),
    realarea character varying(64),
    busiarea character varying(64),
    backstagearea character varying(64),
    callingmachinenum numeric,
    isterminal character varying(2),
    seatnum numeric,
    terminalnum numeric,
    iswisefamilyzone character varying(2),
    isbroadband character varying(2),
    isdownload character varying(2),
    downloadtype character varying(64),
    isviproom character varying(2),
    vipseatnum numeric,
    contractname character varying(256),
    equipmentlist character varying(64),
    contractcode character varying(64),
    currconefftime timestamp(0) without time zone,
    currconfailtime timestamp(0) without time zone,
    contracttext character varying(256),
    integagreement character varying(256),
    mainteamount numeric(10,2),
    creditrating character varying(2),
    taxcert numeric(2,0),
    taxsignid character varying(64),
    taxpregisnum character varying(64),
    registrationno character varying(256),
    lisencename character varying(64),
    lisencefile character varying(256),
    taxregisfile character varying(256),
    rempaybank character varying(64),
    rempaybankid character varying(128),
    rempayperson character varying(64),
    rempayaccountid character varying(128),
    deduofaccountname character varying(64),
    deduofopenaccbank character varying(64),
    recid character varying(64) NOT NULL,
    busitime character varying(512),
    chaincooperid character varying(64),
    supervisorid character varying(64),
    supervisorname character varying(64),
    linkmanname character varying(64),
    linkmantel character varying(128),
    linkmanemail character varying(64),
    receivername character varying(64),
    receivertel character varying(128),
    receiverid character varying(128),
    receiveraddr character varying(256),
    detailaddr character varying(256),
    oppositeimg character varying(256),
    leftimg character varying(256),
    rightimg character varying(256),
    innerimg character varying(256),
    busientityname character varying(64),
    busientitytel character varying(128),
    busientityimg character varying(256),
    isworker character varying(2),
    firconefftime timestamp(0) without time zone,
    currconeffremark character varying(256),
    taxrate numeric(4,2),
    busiautholetter character varying(256),
    realnameagree character varying(256),
    otheragreement character varying(256),
    maintemargin numeric(10,2),
    maintebill character varying(256),
    startbusitime timestamp(0) without time zone,
    closebusitime timestamp(0) without time zone,
    lastupdatetime timestamp(0) without time zone,
    sysefftime timestamp(0) without time zone,
    sysfailtime timestamp(0) without time zone,
    rempaybankname character varying(64),
    rempayacctype character varying(64),
    isrempay character varying(2),
    deduofbankname character varying(64),
    deduofbankaccount character varying(128),
    deduofaccounttype character varying(64),
    createdate timestamp(0) without time zone,
    chaincooperator character varying(64),
    buztypelevel character varying(64),
    apprpastime timestamp(0) without time zone,
    channelcoefficient character varying(16),
    doorimg2 character varying(256),
    yskaccountname character varying(256),
    yskbankname character varying(256),
    yskaccountidcardnum character varying(256),
    yskaccountbank character varying(256),
    yskaccounttype character varying(5),
    yskaccount character varying(256),
    onetimesubsidy numeric(10,2),
    distid character varying(20),
    microdistid character varying(20),
    logisticsid character varying(64),
    partnertype character varying(2) DEFAULT '3'::character varying,
    sourcecode character varying(64),
    sourcename character varying(512),
    deduofcertid character varying(128),
    consigntime timestamp(0) without time zone,
    waymagcode character varying(64),
    waymagname character varying(64),
    ispolysourc numeric(2,0),
    creditlevelfile character varying(16),
    coreareaid character varying(20),
    salebucklebankmobile character varying(20),
    manageroprcode character varying(32),
    busiexpiredate timestamp(0) without time zone,
    storearea numeric(10,2) DEFAULT 0.1 NOT NULL,
    sitescore numeric(4,2),
    bidder numeric(3,2) DEFAULT 1.00 NOT NULL,
    serialid character varying(32),
    sourceplatform character varying(32),
    intoprtype character varying(10),
    longitude character varying(20),
    latitude character varying(20)
)
DISTRIBUTE BY SHARD (managetype);

DROP TABLE IF EXISTS ch_ccm_ownwayinfo;
CREATE TABLE ch_ccm_ownwayinfo (
    recid character varying(16) NOT NULL,
    ownwayid character varying(64),
    creditlevel numeric(5,0),
    businedatescope character varying(512),
    buztypeid character varying(64),
    buztypename character varying(64),
    buztypelevel numeric(2,0),
    propertype numeric(2,0),
    builddate timestamp(0) without time zone,
    purchasearea character varying(64),
    purchaseprice numeric(10,2),
    buildamount numeric(10,2),
    describe character varying(256),
    rentstarttime timestamp(0) without time zone,
    rentendtime timestamp(0) without time zone,
    leasearea character varying(64),
    rentamount numeric(10,2),
    leasesupplemen character varying(256),
    viversion character varying(64),
    signboardheight character varying(64),
    signboardwidth character varying(64),
    backlenth character varying(64),
    backwidth character varying(64),
    fullview character varying(256),
    structurechart character varying(256),
    backimg character varying(256),
    doorheadphoto character varying(256),
    construcimg character varying(256),
    firstdecoratedate timestamp(0) without time zone,
    firstdecorateamt numeric(10,2),
    firstdecoratenum character varying(16),
    firstfurniamt numeric(10,2),
    firstviamt numeric(10,2),
    fitmentdate1 timestamp(0) without time zone,
    recentdecoratype numeric(5,0),
    fitmentamount1 numeric(10,2),
    fitmentcode1 character varying(64),
    recentfurniamt numeric(10,2),
    recentviamt numeric(10,2),
    buildarea character varying(64),
    actualarea character varying(64),
    businearea character varying(64),
    backactualarea character varying(64),
    lineupcount numeric(16,0),
    hasselfservice character varying(5),
    servernumber numeric(16,0),
    selfservicenumber numeric(16,0),
    haswisdom numeric(2,0),
    hasbroadband numeric(2,0),
    candownload numeric(2,0),
    downloadtype character varying(64),
    isvip character varying(5),
    vipseatnumber numeric(16,0),
    equiplist character varying(1024),
    contractname character varying(256),
    contractid character varying(128),
    contractrawdate timestamp(0) without time zone,
    contractlossdate timestamp(0) without time zone,
    contracttext text,
    busineregisternum character varying(64),
    businename character varying(64),
    responsible character varying(64),
    businefile character varying(256),
    taxqualification character varying(16),
    taxregistracard character varying(32),
    taxidentnum character varying(32),
    taxregistrafile character varying(256),
    otherfile character varying(256),
    openbusinesstime timestamp(0) without time zone,
    stopbusinesstime timestamp(0) without time zone,
    transformtime timestamp(0) without time zone,
    restartbusinesstime timestamp(0) without time zone,
    openpasstime timestamp(0) without time zone,
    sysefftime timestamp(0) without time zone,
    sysfailtime timestamp(0) without time zone,
    openattachment character varying(256),
    modifyattachment character varying(256),
    transformattachment character varying(256),
    stopattachment character varying(256),
    dredetail character varying(256),
    auditstate character varying(50),
    type character varying(16),
    county character varying(256),
    consulttel character varying(20),
    messageftright character varying(16),
    aroundinfo character varying(256),
    distid character varying(20),
    microdistid character varying(20),
    businesstype character varying(32),
    consigntime timestamp(0) without time zone,
    creditlevelfile character varying(16),
    coreareaid character varying(20),
    manageroprcode character varying(32),
    busiexpiredate timestamp(0) without time zone,
    sitescore numeric(4,2),
    geoglongitude character varying(20),
    geoglatitude character varying(20)
)
DISTRIBUTE BY SHARD (recid);

DROP TABLE IF EXISTS ch_ccm_intropartners;
CREATE TABLE ch_ccm_intropartners (
    recid character varying(16) NOT NULL,
    cooperatorid character varying(64),
    ownwayid character varying(64),
    ownwayname character varying(256),
    partnerstype character varying(5),
    counteramount numeric(10,0),
    startdate timestamp(0) without time zone,
    partnersbusinedate character varying(512),
    partnersbusinearea character varying(64),
    employeeamount numeric(10,0),
    supervimanagernum character varying(16),
    supervimanagername character varying(16),
    creditlevel character varying(5),
    linkmanname character varying(64),
    linkmantel character varying(128),
    linkmanemail character varying(64),
    receivername character varying(64),
    receivertel character varying(128),
    receiverid character varying(128),
    receiveraddr character varying(256),
    busientityname character varying(64),
    busientitytel character varying(128),
    busientityid character varying(256),
    isworker character varying(2),
    busineregisternum character varying(64),
    businename character varying(64),
    businefile character varying(256),
    contractid character varying(128),
    contractname character varying(256),
    firconefftime timestamp(0) without time zone,
    contractrawdate timestamp(0) without time zone,
    contractlossdate timestamp(0) without time zone,
    currconeffremark character varying(256),
    taxqualification character varying(5),
    taxregistracard character varying(32),
    taxrate numeric(4,2),
    taxidentnum character varying(32),
    contracttext character varying(256),
    businepromise character varying(256),
    taxregistrafile character varying(256),
    busiautholetter character varying(256),
    realnameagree character varying(256),
    otheragreement character varying(256),
    maintemargin numeric(10,2),
    mainteamount numeric(14,2),
    maintebill character varying(256),
    rempaybankname character varying(64),
    rempaybankaccount character varying(64),
    rempaybankid character varying(128),
    rempayperson character varying(64),
    rempaytype character varying(64),
    rempayaccountid character varying(128),
    isrempay character varying(2),
    deduofbankname character varying(64),
    deduofbankcardid character varying(64),
    deduofaccountid character varying(128),
    deduofaccountname character varying(64),
    deduofaccounttype character varying(64),
    startbusitime timestamp(0) without time zone,
    closebusitime timestamp(0) without time zone,
    lastupdatetime timestamp(0) without time zone,
    sysefftime timestamp(0) without time zone,
    sysfailtime timestamp(0) without time zone,
    createdate timestamp(0) without time zone,
    chaincooperid character varying(64),
    channelcoefficient character varying(16),
    yskaccountname character varying(256),
    yskbankname character varying(256),
    yskaccountidcardnum character varying(256),
    yskaccountbank character varying(256),
    yskaccounttype character varying(5),
    yskaccount character varying(256),
    deduofcertid character varying(128),
    consigntime timestamp(0) without time zone,
    ispolysourc numeric(2,0),
    creditlevelfile character varying(16),
    logisticsid character varying(64),
    salebucklebankmobile character varying(20),
    manageroprcode character varying(32),
    busiexpiredate timestamp(0) without time zone,
    tocrmtype character varying(4) DEFAULT 'ET'::character varying NOT NULL,
    subsidy numeric(10,2),
    bidder numeric(3,2),
    dredetail character varying(512),
    longitude character varying(20),
    latitude character varying(20)
)
DISTRIBUTE BY SHARD (recid);

DROP TABLE IF EXISTS ch_td_messagenotice;
CREATE TABLE ch_td_messagenotice (
    oid numeric(14,0) NOT NULL,
    infoid numeric(14,0),
    sender character varying(32) NOT NULL,
    receiver character varying(32) NOT NULL,
    createdate timestamp(0) without time zone NOT NULL,
    state numeric(1,0) NOT NULL,
    noticetype numeric(2,0) NOT NULL,
    describer character varying(1024),
    attrid numeric(14,0)
)
DISTRIBUTE BY SHARD (oid);

DROP TABLE IF EXISTS ch_td_inforeplyrecord;
CREATE TABLE ch_td_inforeplyrecord (
    infoid numeric(14,0) NOT NULL,
    replyid numeric(14,0) NOT NULL,
    replycontent character varying(512) NOT NULL,
    repliedby character varying(32) NOT NULL,
    repliedid numeric(14,0) NOT NULL,
    replyto character varying(32) NOT NULL,
    replytime timestamp(0) without time zone NOT NULL,
    updatetime timestamp(0) without time zone NOT NULL,
    parentreplyid numeric(14,0) NOT NULL,
    isprivated numeric(1,0) NOT NULL,
    istopreply numeric(1,0) NOT NULL,
    state numeric(1,0) DEFAULT 1
)
DISTRIBUTE BY SHARD (infoid);

DROP TABLE IF EXISTS ch_pw_owninterviewatt;
CREATE TABLE ch_pw_owninterviewatt (
    taskid numeric(14,0) NOT NULL,
    picturename character varying(128) NOT NULL,
    picture character varying(128)
)
DISTRIBUTE BY SHARD (taskid);

DROP TABLE IF EXISTS ch_pw_owninterview;
CREATE TABLE ch_pw_owninterview (
    taskid numeric(14,0) NOT NULL,
    taskname character varying(64),
    operid character varying(16) NOT NULL,
    name character varying(64) NOT NULL,
    wayid character varying(18) NOT NULL,
    wayname character varying(128) NOT NULL,
    interviewtype character varying(32) NOT NULL,
    interviewdate timestamp(0) without time zone NOT NULL,
    longitude numeric(10,6),
    latitude numeric(10,6),
    interviewaddress character varying(126),
    diatance character varying(16),
    createdate timestamp(0) without time zone NOT NULL,
    memo character varying(256),
    region numeric(5,0) NOT NULL
)
DISTRIBUTE BY SHARD (taskid);

DROP TABLE IF EXISTS ch_pw_etinspecttaskdet;
CREATE TABLE ch_pw_etinspecttaskdet (
    recid numeric(14,0) NOT NULL,
    taskid character varying(20) NOT NULL,
    version character varying(18) NOT NULL,
    subtaskid character varying(22) NOT NULL,
    staffid character varying(32) NOT NULL,
    wayid character varying(18) NOT NULL,
    substate numeric(3,0) NOT NULL,
    startdate timestamp(0) without time zone NOT NULL,
    enddate timestamp(0) without time zone NOT NULL,
    inspectmode character varying(4) NOT NULL,
    region numeric(5,0) NOT NULL,
    statedate timestamp(0) without time zone NOT NULL,
    memo character varying(256)
)
DISTRIBUTE BY SHARD (recid);

DROP TABLE IF EXISTS ch_pw_etinspectcheck;
CREATE TABLE ch_pw_etinspectcheck (
    recid numeric(14,0) NOT NULL,
    itemid numeric(14,0) NOT NULL,
    oprtype character varying(8) NOT NULL,
    checkresult numeric(3,0) NOT NULL,
    fbconfirm numeric(3,0) NOT NULL,
    complain character varying(300),
    fbstate numeric(3,0) NOT NULL,
    fbenddate timestamp(0) without time zone,
    chresult numeric(3,0) NOT NULL,
    chmemo character varying(256),
    chstate numeric(3,0) NOT NULL,
    resulttype numeric(3,0) NOT NULL,
    oprcode character varying(16),
    oprdate timestamp(0) without time zone,
    inresult numeric(1,0) NOT NULL,
    outresult numeric(1,0) NOT NULL,
    deduct numeric(5,0)
)
DISTRIBUTE BY SHARD (recid);

DROP TABLE IF EXISTS ch_pw_etinspecttask;
CREATE TABLE ch_pw_etinspecttask (
    taskid character varying(20) NOT NULL,
    taskname character varying(64) NOT NULL,
    version character varying(18) NOT NULL,
    region numeric(5,0) NOT NULL,
    creater character varying(32) NOT NULL,
    createdate timestamp(0) without time zone NOT NULL,
    startdate timestamp(0) without time zone NOT NULL,
    enddate timestamp(0) without time zone NOT NULL,
    state numeric(3,0) NOT NULL,
    memo character varying(512),
    statedate timestamp(0) without time zone NOT NULL
)
DISTRIBUTE BY SHARD (taskid);

DROP TABLE IF EXISTS ch_pw_etinspecttaskfb;
CREATE TABLE ch_pw_etinspecttaskfb (
    recid numeric(14,0) NOT NULL,
    itemid numeric(14,0) NOT NULL,
    state numeric(3,0) NOT NULL,
    oprcode character varying(16) NOT NULL,
    inspectdate timestamp(0) without time zone,
    startinspectdate timestamp(0) without time zone,
    endinspectdate timestamp(0) without time zone,
    longitude numeric(10,6),
    latitude numeric(10,6),
    mileage character varying(16),
    inspectresult numeric(1,0) NOT NULL,
    record character varying(512),
    deduct numeric(5,0),
    deductobj character varying(6),
    deductmemo character varying(512)
)
DISTRIBUTE BY SHARD (recid);

DROP TABLE IF EXISTS ch_td_infoattachmentrela;
CREATE TABLE ch_td_infoattachmentrela (
    infoid numeric(14,0) NOT NULL,
    attachmentid character varying(64) NOT NULL,
    attachmentname character varying(128) NOT NULL,
    attachmenttype character varying(128) NOT NULL
)
DISTRIBUTE BY SHARD (infoid);

DROP TABLE IF EXISTS ch_td_generalinfodetail;
CREATE TABLE ch_td_generalinfodetail (
    infoid numeric(14,0) NOT NULL,
    region numeric(5,0) NOT NULL,
    title character varying(512) NOT NULL,
    infotype character varying(32) NOT NULL,
    startdate timestamp(0) without time zone NOT NULL,
    enddate timestamp(0) without time zone NOT NULL,
    neededtrain numeric(1,0),
    isemergent numeric(1,0),
    isimportant numeric(1,0),
    frequency numeric(1,0) NOT NULL,
    isneedfeedback numeric(1,0) NOT NULL,
    module character varying(32) NOT NULL,
    infocolumn character varying(32) NOT NULL,
    draftwayid character varying(32) NOT NULL,
    owner character varying(32) NOT NULL,
    publishtype numeric(1,0) NOT NULL,
    summary character varying(2048),
    content character varying(4000) NOT NULL,
    publishdate timestamp(0) without time zone NOT NULL,
    createdate timestamp(0) without time zone NOT NULL,
    state numeric(2,0) NOT NULL,
    infolabel character varying(32),
    traindate timestamp(0) without time zone,
    approvallevel character varying(3),
    iswaytask numeric(1,0) DEFAULT 0 NOT NULL
)
DISTRIBUTE BY SHARD (infoid);

DROP TABLE IF EXISTS ch_pw_etinspectfbattach;
CREATE TABLE ch_pw_etinspectfbattach (
    oid numeric(14,0) NOT NULL,
    itemid numeric(14,0) NOT NULL,
    taskid character varying(20),
    version character varying(18),
    wayid character varying(18),
    opercode character varying(16),
    attachid character varying(128) NOT NULL,
    attachname character varying(128) NOT NULL,
    attachsize character varying(50),
    state numeric(3,0) NOT NULL,
    attachtype numeric(3,0) NOT NULL,
    attachdate timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (oid);

DROP TABLE IF EXISTS ch_td_interviewtask;
CREATE TABLE ch_td_interviewtask (
    taskid numeric(14,0) NOT NULL,
    taskname character varying(32) NOT NULL,
    issuer character varying(32),
    region numeric(5,0) NOT NULL,
    createdate timestamp(0) without time zone NOT NULL,
    issuedate timestamp(0) without time zone,
    startdate timestamp(0) without time zone,
    enddate timestamp(0) without time zone,
    status character varying(1) NOT NULL,
    interviewtype character varying(300),
    interviewdemand character varying(300),
    feedbackdemand character varying(300),
    interviewlimit character varying(300),
    creator character varying(32) NOT NULL
)
DISTRIBUTE BY SHARD (taskid);

DROP TABLE IF EXISTS sa_db_region;
CREATE TABLE sa_db_region (
    regionid numeric(5,0) NOT NULL,
    regionname character varying(64),
    cityid character varying(16)
)
DISTRIBUTE BY SHARD (regionid);

DROP TABLE IF EXISTS ch_td_interviewdetail;
CREATE TABLE ch_td_interviewdetail (
    detailid numeric(14,0) NOT NULL,
    taskid numeric(14,0),
    opercode character varying(32),
    wayid character varying(32),
    interviewdate timestamp(0) without time zone,
    signindate timestamp(0) without time zone,
    signoutdate timestamp(0) without time zone,
    longitude character varying(12),
    latitude character varying(12),
    interviewcontent character varying(200),
    createdate timestamp(0) without time zone,
    interviewtype character varying(32) NOT NULL
)
DISTRIBUTE BY SHARD (detailid);

DROP TABLE IF EXISTS ch_td_interviewparam;
CREATE TABLE ch_td_interviewparam (
    paramid character varying(32) NOT NULL,
    paramtype character varying(32) NOT NULL,
    content character varying(32) NOT NULL,
    describer character varying(64),
    operid character varying(32),
    region numeric(5,0) NOT NULL,
    createdate timestamp(0) without time zone,
    status character varying(1) NOT NULL
)
DISTRIBUTE BY SHARD (paramid);

DROP TABLE IF EXISTS ch_td_interviewobj;
CREATE TABLE ch_td_interviewobj (
    taskid numeric(14,0) NOT NULL,
    opercode character varying(32) NOT NULL,
    wayid character varying(32) NOT NULL,
    createdate timestamp(0) without time zone NOT NULL,
    status character varying(1) NOT NULL
)
DISTRIBUTE BY SHARD (taskid);

-- Fix core of synonym replacement in transformSequenceColumn
EXPLAIN (costs off) SELECT INT.TASKID,
       INT.TASKNAME,
       (SELECT R.NAME
        FROM SA_SYS_OPERATOR P,
             SA_SYS_STAFFINFO R
        WHERE P.STAFFID = R.STAFFID
          AND P.OPERID = INT.ISSUER) AS ISSUER,
       INT.REGION,
       (SELECT C.REGIONNAME
        FROM SA_DB_REGION C
        WHERE C.REGIONID = INT.REGION) AS REGIONNAME,
       INT.CREATEDATE,
       INT.ISSUEDATE,
       INT.STARTDATE,
       INT.ENDDATE,
       (SELECT T.DICTNAME FROM SA_DB_DICTITEM T WHERE T.DICTID = INT.STATUS AND T.GROUPID = 'INTERVIEWSTATUS') AS STATUS,
       ROUND(((SELECT SUM((CASE WHEN INTERVIEWNUM <= INTERVIEWLIMITNUM THEN TO_NUMBER(INTERVIEWNUM) ELSE TO_NUMBER(INTERVIEWLIMITNUM) END)) INTERVIEWCOUNT 
  FROM(SELECT A.TASKID,A.OPERCODE,A.WAYID,A.INTERVIEWNUM,
  (CASE WHEN C.DESCRIBER IS NOT NULL THEN TO_NUMBER(C.DESCRIBER) ELSE 1 END) INTERVIEWLIMITNUM FROM (SELECT TASKID,OPERCODE,WAYID,COUNT(1) INTERVIEWNUM FROM CH_TD_INTERVIEWDETAIL GROUP BY TASKID,OPERCODE,WAYID) A LEFT JOIN CH_WAY_SOCIETYINFO B ON A.WAYID=B.SOCIETYWAYID AND B.WAYSTATE=1 
							   LEFT JOIN CH_TD_INTERVIEWPARAM C ON B.WAYLEVEL=C.CONTENT AND C.PARAMTYPE='NUMBERLIMIT')
               WHERE TASKID= INT.TASKID GROUP BY TASKID) / (SELECT SUM((CASE WHEN T.DESCRIBER IS NOT NULL THEN TO_NUMBER(T.DESCRIBER) ELSE 1 END)) DESCRIBER
               FROM (SELECT A.TASKID, B.SOCIETYWAYID, B.WAYLEVEL, S.DESCRIBER
                     FROM CH_TD_INTERVIEWOBJ A
                              join CH_WAY_SOCIETYINFO B on A.WAYID = B.SOCIETYWAYID
                              LEFT JOIN CH_TD_INTERVIEWPARAM S ON TO_CHAR(B.WAYLEVEL) = S.CONTENT) T
               where T.TASKID = INT.TASKID)) * 100,
             1) PERCENTCOMPLETE,
       INT.INTERVIEWTYPE,
       INT.INTERVIEWDEMAND,
       INT.FEEDBACKDEMAND,
       INT.INTERVIEWLIMIT FROM CH_TD_INTERVIEWTASK INT 
	   WHERE 1=1 AND INT.REGION=200 
	   AND INT.STATUS=1 
	   UNION
	   SELECT W.TASKID,W.TASKNAME,(SELECT R.NAME FROM SA_SYS_OPERATOR P, SA_SYS_STAFFINFO R WHERE P.STAFFID=R.STAFFID AND P.OPERID = W.ISSUER) AS ISSUER,W.REGION,
	   (SELECT C.REGIONNAME FROM SA_DB_REGION C WHERE C.REGIONID=W.REGION) AS REGIONNAME,W.CREATEDATE,W.ISSUEDATE,W.STARTDATE,W.ENDDATE,
	   (SELECT T.DICTNAME FROM SA_DB_DICTITEM T WHERE T.DICTID = W.STATUS AND T.GROUPID = 'INTERVIEWSTATUS') AS STATUS,
	   ROUND(((SELECT SUM((CASE WHEN INTERVIEWNUM<=INTERVIEWLIMITNUM THEN INTERVIEWNUM ELSE TO_NUMBER(INTERVIEWLIMITNUM) END)) INTERVIEWCOUNT 
	   FROM(SELECT A.TASKID,A.OPERCODE,A.WAYID,A.INTERVIEWNUM,(CASE WHEN C.DESCRIBER IS NOT NULL THEN TO_NUMBER(C.DESCRIBER) ELSE 1 END) INTERVIEWLIMITNUM
	   FROM (SELECT TASKID,OPERCODE,WAYID,COUNT(1) INTERVIEWNUM FROM CH_TD_INTERVIEWDETAIL GROUP BY TASKID,OPERCODE,WAYID) A
	   LEFT JOIN CH_WAY_SOCIETYINFO B ON A.WAYID=B.SOCIETYWAYID AND B.WAYSTATE=1 
	   LEFT JOIN CH_TD_INTERVIEWPARAM C ON B.WAYLEVEL=C.CONTENT AND C.PARAMTYPE='NUMBERLIMIT') 
	   WHERE TASKID=W.TASKID GROUP BY TASKID)/(SELECT SUM((CASE WHEN T.DESCRIBER IS NOT NULL THEN TO_NUMBER(T.DESCRIBER) ELSE 1 END)) DESCRIBER FROM (SELECT A.TASKID,B.SOCIETYWAYID,B.WAYLEVEL,S.DESCRIBER FROM CH_TD_INTERVIEWOBJ A 
	   join CH_WAY_SOCIETYINFO B on A.WAYID=B.SOCIETYWAYID LEFT JOIN CH_TD_INTERVIEWPARAM S ON TO_CHAR(B.WAYLEVEL) =S.CONTENT) T 
	   where T.TASKID=W.TASKID))*100,1) PERCENTCOMPLETE,W.INTERVIEWTYPE,W.INTERVIEWDEMAND,W.FEEDBACKDEMAND,W.INTERVIEWLIMIT FROM CH_TD_INTERVIEWTASK W WHERE 1=1 AND W.STATUS != 1 AND W.REGION=200
ORDER BY 7 desc;

-- Pull up subquery and CTE inline
EXPLAIN (costs off) WITH ITEMPHOTO AS(
        SELECT ITEMID, CODE AS PHOTONAME 
        FROM (SELECT ITEMID, CODE FROM CH_PW_ETINSPECTPIC)
        GROUP BY ITEMID, PHOTONAME),
     ITEMINFO AS(
        SELECT A.ITEMID, A.VERSION, A.INSPECTTYPE, A.LEVTWOSTD,
            A.LEVTHRSTD, A.CONTENT, A.UPDATEDETAIL, A.INSPECTMODE,
            A.SCORE, A.INSPECTOBJ, B.RESULT,
            (SELECT DICTNAME FROM SA_DB_DICTITEM WHERE GROUPID='EtInspectRecordResult' AND DICTID=to_char(B.RESULT)) AS RESULTNAME,
            B.STATE,
            (SELECT DICTNAME FROM SA_DB_DICTITEM WHERE GROUPID='EtInspectRecordState' AND DICTID=to_char(B.STATE)) AS STATENAME 
        FROM CH_PW_ETINSPECTITEMSTD A LEFT JOIN CH_PW_ETINSPECTRCD B ON A.ITEMID=B.ITEMID AND A.VERSION=B.VERSION AND B.WAYID='A'
        WHERE A.VERSION IN(
            SELECT VERSION 
            FROM CH_PW_ETINSPECTSTD 
            WHERE PUBDATE IN (
                SELECT MAX(PUBDATE) FROM CH_PW_ETINSPECTSTD WHERE STATE=1 AND REGION=1 )
        )AND A.REGION=2)
SELECT * FROM 
    (SELECT C.*, D.PHOTONAME FROM ITEMINFO C LEFT JOIN ITEMPHOTO D ON C.ITEMID=D.ITEMID) E WHERE 1=1;

EXPLAIN (costs off) WITH WAYINFO AS
    (
        SELECT DATAID AS DICTID, MSGINFO AS DICTNAME
        FROM T_BME_PUBLICDATADICT T1, T_BME_LANGUAGELOCALDISPLAY T2
        WHERE T1.DATADESC = T2.KEYINDEX AND T1.TYPEID LIKE '%CH.NEW.WAYTYPE%'
    )
SELECT OPERLOG.LOGID, OPERATOR.OPERNAME, OPERLOG.OPERTIME, MENU.NAME, OPERLOG.OPERTYPE OPERTYPE,
    OPERLOG.OPERDESC OPERDESC, OPERLOG.SYSIN, OPERATOR.STAFFID, OPERATOR.OPERID, OPERATOR.CONTACTPHONE, 
    OPERATOR.ORGID,
    (
        SELECT SOCIETYWAYNAME FROM CH_WAY_SOCIETYINFO WHERE SOCIETYWAYID = OPERATOR.ORGID
    ) AS SOCIETYWAYNAME,
    (
        CASE WHEN 
            EXISTS (SELECT 1 FROM CH_CCM_SOCIETYINFO WHERE SOCIETYWAYID = OPERATOR.ORGID) 
        THEN
            (SELECT DISTNAME FROM CH_WAY_DISTRICT WHERE DISTID = (SELECT DISTID FROM CH_CCM_SOCIETYINFO WHERE SOCIETYWAYID = OPERATOR.ORGID))
        WHEN 
            EXISTS (SELECT 1 FROM CH_CCM_FRANCHISE WHERE WAYID = OPERATOR.ORGID) 
        THEN
            (SELECT DISTNAME FROM CH_WAY_DISTRICT WHERE DISTID = (SELECT DISTID FROM CH_CCM_FRANCHISE WHERE WAYID = OPERATOR.ORGID))
        WHEN 
            EXISTS (SELECT 1 FROM CH_CCM_OWNWAYINFO WHERE OWNWAYID = OPERATOR.ORGID) 
        THEN
            (SELECT DISTNAME FROM CH_WAY_DISTRICT WHERE DISTID = (SELECT DISTID FROM CH_CCM_OWNWAYINFO WHERE OWNWAYID = OPERATOR.ORGID))
        WHEN 
            EXISTS (SELECT 1 FROM CH_CCM_INTROPARTNERS WHERE COOPERATORID = OPERATOR.ORGID)
        THEN
            (SELECT DISTNAME FROM CH_WAY_DISTRICT WHERE DISTID = (SELECT DISTID FROM CH_CCM_OWNWAYINFO WHERE OWNWAYID = (SELECT OWNWAYID FROM CH_CCM_INTROPARTNERS WHERE COOPERATORID = OPERATOR.ORGID)))
        ELSE'-'END
    ) AS DISTNAME,
    (
        SELECT WAYTYPE FROM CH_WAY_SOCIETYINFO WHERE SOCIETYWAYID = OPERATOR.ORGID
    ) AS WAYTYPE,
    (
        SELECT DICTNAME FROM WAYINFO INNER JOIN CH_WAY_SOCIETYINFO ON WAYTYPE = DICTID WHERE SOCIETYWAYID = OPERATOR.ORGID
    ) AS WAYTYPENAME,
    (
        CASE WHEN 
            EXISTS (SELECT 1 FROM CH_CCM_SOCIETYINFO WHERE SOCIETYWAYID = OPERATOR.ORGID) 
        THEN
            (SELECT CHAINCOOPERATOR FROM CH_CCM_SOCIETYINFO WHERE SOCIETYWAYID = OPERATOR.ORGID)
        WHEN 
            EXISTS (SELECT 1 FROM CH_CCM_FRANCHISE WHERE WAYID = OPERATOR.ORGID)
        THEN
            (SELECT CHAINCOOPERATOR FROM CH_CCM_FRANCHISE WHERE WAYID = OPERATOR.ORGID)
        ELSE'-'END
    ) AS CHAINCOOPERATOR
FROM SA_SYS_OPERLOG OPERLOG INNER JOIN SA_SYS_OPERATOR OPERATOR
ON OPERLOG.OPERID = OPERATOR.OPERID INNER JOIN SA_SYS_MENU MENU
ON OPERLOG.MENUID = MENU.MENUID and MENU.status = 1 INNER JOIN CH_WAY_SOCIETYINFO SOCIETYINFO
ON OPERATOR.ORGID = SOCIETYINFO.SOCIETYWAYID
WHERE 1 = 1 AND TO_CHAR(OPERLOG.OPERTIME, 'YYYY-MM-DD') >= 'YYYY-MM-DD'
    AND TO_CHAR(OPERLOG.OPERTIME, 'YYYY-MM-DD') <= 'YYYY-MM-DD'
ORDER BY OPERLOG.OPERTIME DESC;

EXPLAIN (costs off) WITH BASE AS(
    SELECT 
        MESSAGENOTICE.OID, GENERALINFODETAIL.TITLE, INFOREPLYRECORD.REPLYCONTENT, MESSAGENOTICE.SENDER, 
        (
            SELECT OPERATOR.OPERNAME
            FROM SA_SYS_OPERATOR OPERATOR
            WHERE OPERATOR.OPERID = MESSAGENOTICE.SENDER
        ) SENDERNAME,
        (
            SELECT OPERATOR.REGION
            FROM SA_SYS_OPERATOR OPERATOR
            WHERE OPERATOR.OPERID = MESSAGENOTICE.SENDER
        ) SENDERREGION,
        MESSAGENOTICE.CREATEDATE, GENERALINFODETAIL.OWNER,
        (
            SELECT SOCIETYINFO.SOCIETYWAYNAME
            FROM CH_WAY_SOCIETYINFO SOCIETYINFO
            INNER JOIN SA_SYS_OPERATOR OPERATOR
            ON OPERATOR.ORGID = SOCIETYINFO.SOCIETYWAYID
            WHERE OPERATOR.OPERID = MESSAGENOTICE.SENDER
        ) AS POSTNAME,
        GENERALINFODETAIL.REGION, GENERALINFODETAIL.INFOTYPE, GENERALINFODETAIL.INFOID,
        MESSAGENOTICE.DESCRIBER AS TASKID,
        GENERALINFODETAIL.ISIMPORTANT, INFOREPLYRECORD.PARENTREPLYID, INFOREPLYRECORD.REPLYID,
        INFOREPLYRECORD.REPLIEDBY, INFOREPLYRECORD.ISPRIVATED,
        (
            SELECT TASKDETAIL.STATE
            FROM CH_TD_TASKDETAIL TASKDETAIL
            WHERE TASKDETAIL.TASKID = MESSAGENOTICE.DESCRIBER AND TASKDETAIL.STATE <> '3' 
        ) AS TASKDETAILSTATE,
        (
            SELECT TASKDETAIL.TASKTYPE
            FROM CH_TD_TASKDETAIL TASKDETAIL
            WHERE TASKDETAIL.TASKID = MESSAGENOTICE.DESCRIBER AND TASKDETAIL.STATE <> '3' 
        ) AS TASKTYPE,
        (
            SELECT TASKDETAIL.ISMULTIWAY
            FROM CH_TD_TASKDETAIL TASKDETAIL
            WHERE TASKDETAIL.TASKID = MESSAGENOTICE.DESCRIBER AND TASKDETAIL.STATE <> '3'
        ) AS ISMULTIWAY,
        (
            SELECT INFORECEIVERRELA.ISREAD
            FROM CH_TD_INFORECEIVERRELA INFORECEIVERRELA
            WHERE INFORECEIVERRELA.INFOID = GENERALINFODETAIL.INFOID AND INFORECEIVERRELA.RECEIVER = MESSAGENOTICE.RECEIVER
        ) AS ISREAD,
        MESSAGENOTICE.STATE
        FROM CH_TD_MESSAGENOTICE MESSAGENOTICE INNER JOIN CH_TD_GENERALINFODETAIL GENERALINFODETAIL ON MESSAGENOTICE.INFOID = GENERALINFODETAIL.INFOID
        INNER JOIN CH_TD_INFOREPLYRECORD INFOREPLYRECORD ON MESSAGENOTICE.ATTRID = INFOREPLYRECORD.REPLYID
        WHERE MESSAGENOTICE.NOTICETYPE = '3' AND INFOREPLYRECORD.STATE ='1' AND MESSAGENOTICE.RECEIVER = 1 AND 
            (GENERALINFODETAIL.MODULE != 2 OR GENERALINFODETAIL.INFOTYPE != 3)
        ORDER BY MESSAGENOTICE.CREATEDATE DESC)
SELECT 
    BASE.OID, BASE.TITLE, BASE.REPLYCONTENT, BASE.SENDER, BASE.SENDERNAME, BASE.SENDERREGION, BASE.CREATEDATE, BASE.OWNER, 
    BASE.POSTNAME, BASE.REGION, BASE.INFOTYPE, BASE.INFOID, BASE.TASKID, BASE.ISIMPORTANT, BASE.PARENTREPLYID, BASE.REPLYID,
    BASE.REPLIEDBY, BASE.ISPRIVATED, BASE.TASKDETAILSTATE, BASE.TASKTYPE, BASE.ISMULTIWAY, BASE.ISREAD, BASE.STATE
FROM BASE
WHERE BASE.TASKDETAILSTATE <> '2'OR BASE.TASKDETAILSTATE IS NULL
LIMIT 10 offset 1;

EXPLAIN (costs off) With BASE AS (
        SELECT taskdetail.TASKID,taskdetail.INFOID,infodetail.TITLE,infodetail.CONTENT,taskdetail.STATE,
            taskdetail.APPROVALLEVEL,
            (
                SELECT COUNT(*) FROM CH_TD_TASKDETAIL detail 
                WHERE detail.infoid =taskdetail.infoid AND detail.tasktype ='0' AND detail.state in ('0','1') AND 
                    ((detail.ISMULTIWAY=1) or (detail.ISMULTIWAY=0 AND detail.WAYID IS NULL))
            ) AS TOTALTASKCOUNT,
            (
                SELECT COUNT(*) FROM CH_TD_TASKDETAIL detail 
                WHERE detail.infoid =taskdetail.infoid and detail.tasktype ='0' and detail.state ='1' and 
                    ((detail.ISMULTIWAY=1) or (detail.ISMULTIWAY=0 AND detail.WAYID IS NULL))
            ) AS FINISHEDCOUNT,
            infodetail.OWNER,
            (
                select OPERNAME from SA_SYS_OPERATOR sysoper where sysoper.OPERID =infodetail.OWNER
            ) as OWNERNM,
            infodetail.PUBLISHDATE,taskdetail.ENDDATE AS TASKENDDATE,infodetail.ENDDATE AS INFOENDDATE,
            (
                SELECT COUNT(*) FROM CH_TD_INFOREPLYRECORD replyrecord WHERE replyrecord.infoid =taskdetail.infoid and replyrecord.REPLIEDID ='-1'
            ) AS REPLIEDIDCOUNT,
            (
                SELECT COUNT(*) FROM CH_TD_INFOATTACHMENTRELA attachmentrela WHERE attachmentrela.infoid =taskdetail.infoid
            ) AS ATTACHMENTCOUNT,
            taskdetail.WAYID,taskdetail.ISMULTIWAY,taskdetail.DISPATCHEDBY as DISPATCHEDID,
            (
                SELECT OPERNAME FROM SA_SYS_OPERATOR sysoper WHERE sysoper.OPERID =taskdetail.DISPATCHEDBY
            ) AS DISPATCHEDNM,
            (
                SELECT SOCIETYINFO.SOCIETYWAYNAME FROM SA_SYS_OPERATOR OPERATOR INNER JOIN CH_WAY_SOCIETYINFO SOCIETYINFO 
                ON SOCIETYINFO.SOCIETYWAYID = OPERATOR.ORGID WHERE OPERATOR.OPERID = taskdetail.DISPATCHEDBY
            ) AS DISPATCHEDSOCIETYWAYNM,
            taskdetail.UPDATEDATE as DISPATCHEDDATE,infodetail.infocolumn as INFOCOLUMNCD,
            (
                SELECT dict.dictname FROM SA_DB_DICTITEM dict WHERE dict.dictid =infodetail.infocolumn and dict.region =999 and dict.groupid='ColumnType'
            ) AS INFOCOLUMNNM,
            infodetail.REGION,
            (
                CASE WHEN infodetail.ENDDATE < sysdate THEN 1 ELSE 0 END
            ) AS PULLOFFFLAG,
            (
                SELECT SOCIETYINFO.SOCIETYWAYNAME FROM SA_SYS_OPERATOR OPERATOR INNER JOIN CH_WAY_SOCIETYINFO SOCIETYINFO 
                ON SOCIETYINFO.SOCIETYWAYID = OPERATOR.ORGID
                WHERE OPERATOR.OPERID = infodetail.OWNER
            ) AS SOCIETYWAYNAME,
            infodetail.CREATEDATE,
            (
                CASE WHEN (
                    SELECT COUNT(*) FROM CH_TD_MESSAGENOTICE mn WHERE mn.INFOID = infodetail.INFOID AND mn.ATTRID = taskdetail.TASKID AND mn.RECEIVER = taskdetail.RECEIVER AND
                        mn.STATE = 0 AND mn.NOTICETYPE = 1 ) >0 THEN 1 ELSE 0 END
            ) as URGEFLAG 
        FROM CH_TD_TASKDETAIL taskdetail INNER JOIN CH_TD_GENERALINFODETAIL infodetail ON taskdetail.infoid =infodetail.infoid Where taskdetail.RECEIVER = 2
            AND infodetail.state ='3' AND taskdetail.tasktype =0 AND taskdetail.state =0 AND taskdetail.issubtask =0 And infodetail.infotype ='task' AND 
            infodetail.MODULE NOT IN('xccx')
        order by infodetail.CREATEDATE desc)
SELECT TASKID,INFOID,TITLE,STATE,FLOOR(FINISHEDCOUNT/TOTALTASKCOUNT*100) FINISHEDRATE,FINISHEDCOUNT,TOTALTASKCOUNT,OWNER,OWNERNM,PUBLISHDATE,TASKENDDATE,INFOENDDATE,
    REPLIEDIDCOUNT,ATTACHMENTCOUNT,WAYID,ISMULTIWAY,DISPATCHEDID,DISPATCHEDNM,DISPATCHEDSOCIETYWAYNM,DISPATCHEDDATE,INFOCOLUMNCD,INFOCOLUMNNM,REGION,PULLOFFFLAG,
    SOCIETYWAYNAME,URGEFLAG
FROM BASE 
Where 1 =1
ORDER BY CREATEDATE desc
limit 10 offset 1;

EXPLAIN (costs off) WITH COOPERATOR AS(
        SELECT OWNWAYID, COOPERATORNAME AS COOPERATORS
        FROM (
            SELECT OWNWAYID,
                (
                    SELECT SOCIETYWAYNAME FROM CH_WAY_SOCIETYINFO
                    WHERE COOPERATORID = SOCIETYWAYID) AS COOPERATORNAME
            FROM CH_CCM_INTROPARTNERS)
            WHERE 1=1 
            GROUP BY OWNWAYID, COOPERATORS),
    WAYINFO AS(
        SELECT DISTINCT 
            A.WAYID, D.DISTNAME, B.SOCIETYWAYNAME,
            (
                SELECT B.MSGINFO MSGINFO
                FROM t_bme_publicdatadict T, T_BME_LANGUAGELOCALDISPLAY B 
                WHERE T.DATADESC = B.KEYINDEX AND T.TYPEID = 'CH.PARTNERSTYPE' AND T.DATAID=C.TYPE) AS TYPENAME
        FROM CH_PW_ETINSPECTTASKDET A LEFT JOIN CH_WAY_SOCIETYINFO B 
        ON A.WAYID = B.SOCIETYWAYID LEFT JOIN CH_CCM_OWNWAYINFO C 
        ON C.OWNWAYID = A.WAYID LEFT JOIN CH_WAY_DISTRICT D ON D.DISTID = C.DISTID
        WHERE 1=1 AND EXTRACT(YEAR FROM A.STATEDATE)=EXTRACT(YEAR FROM TO_DATE('201701' ,'yyyy/mm'))
            AND EXTRACT(MONTH FROM A.STATEDATE)=EXTRACT(MONTH FROM TO_DATE('201701' ,'yyyy/mm'))),
    GRADEA AS(
        SELECT A.WAYID, SUM(B.DEDUCT) ATOTAL
        FROM CH_PW_ETINSPECTTASKDET A, CH_PW_ETINSPECTCHECK B 
        WHERE A.RECID = B.RECID AND A.INSPECTMODE = 'A' AND A.SUBSTATE IN (6, 9) AND B.OPRTYPE = '4'
            AND EXTRACT(YEAR FROM A.STATEDATE)=EXTRACT(YEAR FROM TO_DATE('201701' ,'yyyy/mm'))AND 
            EXTRACT(MONTH FROM A.STATEDATE)=EXTRACT(MONTH 
        FROM TO_DATE('201701' ,'yyyy/mm'))
        GROUP BY A.WAYID),
    GRADEM AS(
        SELECT A.WAYID, SUM(B.DEDUCT) MTOTAL
        FROM CH_PW_ETINSPECTTASKDET A,CH_PW_ETINSPECTCHECK B
        WHERE A.RECID = B.RECID AND A.INSPECTMODE = 'M'AND A.SUBSTATE IN (6, 9)AND B.OPRTYPE = '4' AND 
            EXTRACT(YEAR FROM A.STATEDATE)=EXTRACT(YEAR FROM TO_DATE('201701' ,'yyyy/mm')) AND
            EXTRACT(MONTH FROM A.STATEDATE)=EXTRACT(MONTH FROM TO_DATE('201701' ,'yyyy/mm'))
        GROUP BY A.WAYID, A.STATEDATE)
SELECT WAYINFO.WAYID, WAYINFO.DISTNAME, WAYINFO.SOCIETYWAYNAME, COOPERATOR.COOPERATORS, WAYINFO.TYPENAME, '201701' STATEDATE, GRADEA.ATOTAL, GRADEM.MTOTAL,
    (GRADEA.ATOTAL+GRADEM.MTOTAL) TOTAL
FROM WAYINFO LEFT JOIN COOPERATOR 
ON WAYINFO.WAYID=COOPERATOR.OWNWAYID LEFT JOIN GRADEA ON WAYINFO.WAYID=GRADEA.WAYID
LEFT JOIN GRADEM ON WAYINFO.WAYID=GRADEM.WAYID;

EXPLAIN (costs off) WITH BASE AS (
        SELECT 
            TASK.RULEID, TASK.CYCLE, TASK.REGION, REC.TOTAL, TASK.SUCCEED, TASK.FAILED, 
            REC.STATUSTIME, REC.STATUS AS TASKSTATUS, REC.RUNTIME, RULEDEF.RULENAME, 
            POLICYDEF.POLICYNAME, RULEFLOW.RULEMODE ruleMode, RULEFLOW.STATUS, 
            RULEFLOW.REGION RULEFLOWREGION, 
            CASE  WHEN REC.TOTAL = 0 
                THEN 0 
                ELSE FLOOR((TASK.SUCCEED + TASK.FAILED) * 100 / REC.TOTAL) 
                END AS COMPLETEPROCESS, 
            REWARDACTVAL / 100 AS TOTALAMOUNT 
        FROM (
            SELECT 
                SUM(SUCCNUM) SUCCEED, SUM(FAILNUM) FAILED, sum(REWARDACTVAL) REWARDACTVAL, 
                RULEID, REGION, CYCLE FROM (SELECT count(1) SUCCNUM, 0 FAILNUM, 
                SUM(detail.REWARDACTVAL) REWARDACTVAL, detail.ruleid, detail.region, detail.cycle 
            FROM 
                RD_ST_SUBDETAIL detail 
            WHERE 
                detail.region = 1 AND detail.cycle = 2 
            GROUP BY detail.region, detail.cycle, detail.ruleid 
            UNION ALL 
            SELECT 
                0 SUCCNUM, COUNT(1) FAILNUM, 0 REWARDACTVAL, fail.ruleid, fail.region, fail.cycle 
            FROM 
                RD_ST_FAILURELOG fail 
            WHERE 
                fail.region = 1 AND fail.cycle = 2 
            GROUP BY fail.region, fail.cycle, fail.ruleid)
        GROUP BY ruleid, region, CYCLE) TASK 
        LEFT JOIN (
            SELECT 
                a.STATUSTIME, a.STATUS, a.taskid, b.taskcode, ordercnt total, REGION, 
                CYCLE, (a.STATUSTIME - a.STARTTIME) * 86400 AS RUNTIME 
            FROM 
                rd_st_rewardprocrec a JOIN rd_ST_RECALCAPPLYDETAIL b 
            ON 
                (a.taskid = b.applyoid AND b.classifyid = 'REWARDCALC') 
            WHERE a.process = 'Settle') REC 
        ON (TASK.cycle = REC.cycle AND TASK.region = REC.region AND REC.taskcode = TASK.ruleid) 
        LEFT JOIN RD_CF_RULEDEF RULEDEF ON RULEDEF.RULEID = TASK.RULEID AND RULEDEF.CYCLE = TASK.CYCLE AND RULEDEF.STATUS = 1 
        LEFT JOIN RD_CF_POLICYDEF POLICYDEF ON POLICYDEF.POLICYID = RULEDEF.BELONCOMMSID AND POLICYDEF.STATUS IN (2, 3) 
        LEFT JOIN RD_CF_RULEFLOW RULEFLOW ON RULEFLOW.RULEID = TASK.RULEID AND RULEFLOW.CYCLE = TASK.CYCLE AND 
            (RULEFLOW.REGION = TASK.REGION OR RULEFLOW.REGION = 999) AND RULEFLOW.STATUS IN (1, 6, 8) 
        WHERE RULEFLOW.RULEFLOWID IN (
            SELECT FLOWDEF.RULEFLOWID FROM RD_CF_FLOWDEF FLOWDEF 
            WHERE FLOWDEF.RULEFLOWID = RULEFLOW.RULEFLOWID AND FLOWDEF.CYCLE = RULEFLOW.CYCLE AND FLOWDEF.FLOWTYPE = 'CALCPROC') 
        AND TASK.CYCLE = 1 AND TASK.REGION = 2), 
    TAB_BASE AS (
        SELECT
            B.RULEID, B.CYCLE, B.REGION, B.TOTAL, B.SUCCEED, B.FAILED, B.STATUSTIME, B.STATUS, 
            B.RULEFLOWREGION, B.RULENAME, B.POLICYNAME, B.COMPLETEPROCESS, B.RUNTIME, B.TOTALAMOUNT, 
            B.TASKSTATUS, B.RULEMODE, ROW_NUMBER() OVER (PARTITION BY B.RULEID, B.CYCLE, B.REGION ORDER BY B.RULEFLOWREGION ASC) RANK FROM BASE B)
    SELECT count(0) FROM TAB_BASE WHERE RANK = 1;
    
EXPLAIN (costs off) WITH COEFINFO AS (
    SELECT 
        distinct SUB.CYCLE, SUB.RECORGID, WY.SOCIETYWAYNAME, CF.TYPE, CF.COEFFICIENT
    FROM 
        CH_NGSETTLE_SUBDETAIL SUB, AGENT AG, CH_WAY_SOCIETYINFO WY,CH_BL_WAYCOEFFICIENT CF
    WHERE 
        SUB.RECORGID = AG.AGENTID AND SUB.CYCLE = AG.CYCLE AND 
        SUB.RECORGID = WY.SOCIETYWAYID AND SUB.CYCLE = CF.CYCLE AND
        SUB.RECORGID = CF.WAYID AND 
        AG.WAYTYPE = 'AG'AND SUB.REGION = 1 AND AG.REGION = 2 AND CF.CYCLE = 3)
SELECT 
    distinct COEFINFO.RECORGID AS WAYID, COEFINFO.CYCLE, COEFINFO.SOCIETYWAYNAME AS WAYNAME, 
    (SELECT SUM(REWARDVAL)/100 FROM CH_NGSETTLE_SUBDETAIL WHERE (SUBSTR(OPNID,1,2) = 'SH' OR SUBSTR(OPNID,1,2) = 'ZD' ) AND RECORGID = COEFINFO.RECORGID ) BASEREWARDVAL,
    (SELECT SUM(REWARDVAL)/100 FROM CH_NGSETTLE_SUBDETAIL WHERE SUBSTR(OPNID,1,2) = 'DL' AND RECORGID = COEFINFO.RECORGID) TASKREWARDVAL,
    (SELECT SUM(REWARDVAL)/100 FROM CH_NGSETTLE_SUBDETAIL WHERE OPNID = 'DB1010001' AND RECORGID = COEFINFO.RECORGID) CXJYREWARDVAL,
    (SELECT SUM(REWARDVAL)/100 FROM CH_NGSETTLE_SUBDETAIL WHERE OPNID IN('DB1020001','JL1010001','JL1020001','JL1030001') AND RECORGID = COEFINFO.RECORGID) JLREWARDVAL
FROM 
    COEFINFO 
WHERE 1 = 1;

EXPLAIN (costs off) WITH TASKCHECKONE AS(
        SELECT RECID, ITEMID, CHECKRESULT 
        FROM CH_PW_ETINSPECTCHECK 
        WHERE OPRTYPE = '1'),
    TASKCHECKTWO AS(
        SELECT RECID, ITEMID, COMPLAIN,FBSTATE,FBENDDATE,FBCONFIRM,OUTRESULT,OPRDATE,OPRCODE 
        FROM CH_PW_ETINSPECTCHECK 
        WHERE OPRTYPE = '2'),
    TASKCHECKFOUR AS(
        SELECT RECID, ITEMID, CHRESULT,OPRDATE,OPRCODE,DEDUCT,CHMEMO 
        FROM CH_PW_ETINSPECTCHECK 
        WHERE OPRTYPE = '4')
SELECT 
    TASKINFO.TASKID,TASKINFO.RECID,TASKINFO.WAYID,TASKINFO.INSPECTDATE,TASKINFO.STARTINSPECTDATE,TASKINFO.ENDINSPECTDATE,
    TASKINFO.STAFFID,TASKINFO.INSPECTRESULT,TASKINFO.DEDUCT AS SCORE,TASKINFO.DEDUCTOBJ,TASKINFO.DEDUCTMEMO,TASKINFO.INSPECTMODE,
    TASKINFO.VERSION,TASKINFO.STATE KFBSTATE,TASKINFO.ITEMID,TASKINFO.INSPECTTYPE,TASKINFO.LEVTWOSTD,TASKINFO.LEVTHRSTD,
    TASKINFO.SUBSTATE,TASKCHECKONE.CHECKRESULT,TASKCHECKTWO.COMPLAIN,TASKCHECKTWO.FBSTATE,TASKCHECKTWO.FBENDDATE,TASKCHECKTWO.FBCONFIRM,
    TASKCHECKTWO.OPRCODE OPRCODEOFAPPEAL,TASKCHECKTWO.OPRDATE OPRDATEOFAPPEAL,TASKCHECKTWO.OUTRESULT OUTRESULTOFSECOND,TASKCHECKFOUR.CHRESULT AS APPEALRESULT,
    TASKCHECKFOUR.CHMEMO AS COMPLAINCOMMENT,TASKCHECKFOUR.DEDUCT,TASKCHECKFOUR.OPRCODE,TASKCHECKFOUR.OPRDATE,
    (
        SELECT COUNT(1) FROM CH_PW_ETINSPECTFBATTACH FBATTACH 
        WHERE 
            FBATTACH.TASKID = TASKINFO.TASKID AND FBATTACH.WAYID = TASKINFO.WAYID AND 
            FBATTACH.OPERCODE = TASKINFO.STAFFID AND FBATTACH.ITEMID = TASKINFO.ITEMID AND 
            FBATTACH.VERSION = TASKINFO.VERSION AND FBATTACH.ATTACHTYPE = 4 
    ) ISATTACH,
    (
        SELECT COUNT(1) FROM CH_PW_ETINSPECTFBATTACH FBATTACH
        WHERE 
            FBATTACH.TASKID = TASKINFO.TASKID AND FBATTACH.WAYID = TASKINFO.WAYID AND
            FBATTACH.OPERCODE = TASKINFO.STAFFID AND FBATTACH.ITEMID = TASKINFO.ITEMID AND
            FBATTACH.VERSION = TASKINFO.VERSION AND FBATTACH.ATTACHTYPE = 5 
    ) ISCMATTACH,
    (
        SELECT COUNT(1) FROM CH_PW_ETINSPECTFBATTACH FBATTACH 
        WHERE 
            FBATTACH.TASKID = TASKINFO.TASKID AND FBATTACH.WAYID = TASKINFO.WAYID AND
            FBATTACH.OPERCODE = TASKINFO.STAFFID AND FBATTACH.ITEMID = TASKINFO.ITEMID AND
            FBATTACH.VERSION = TASKINFO.VERSION AND FBATTACH.ATTACHTYPE = 6 
    ) ISCHATTACH,
    (
        SELECT COUNT(1) FROM CH_PW_ETINSPECTCHECK INSPECTCHECK
        WHERE 
            INSPECTCHECK.RECID = TASKINFO.RECID AND INSPECTCHECK.ITEMID = TASKINFO.ITEMID AND
            INSPECTCHECK.OPRTYPE = '4' 
    ) ISHANDLE,
    (
        SELECT NAME FROM SA_SYS_STAFFINFO 
        WHERE STAFFID = TASKINFO.STAFFID
    ) SANAME,
    (
        SELECT SOCIETYWAYNAME FROM CH_WAY_SOCIETYINFO WHERE SOCIETYWAYID = TASKINFO.WAYID
    ) SOCIETYWAYNAME
FROM (
    SELECT 
        TASK.TASKID,TASKDET.RECID,TASKDET.WAYID,TASKFB.INSPECTDATE,TASKFB.STARTINSPECTDATE,
        TASKFB.ENDINSPECTDATE,TASKDET.STAFFID,TASKDET.SUBSTATE,TASKFB.INSPECTRESULT,TASKFB.DEDUCT,
        TASKFB.DEDUCTOBJ,TASKFB.DEDUCTMEMO,TASKDET.INSPECTMODE,TASK.VERSION,TASKFB.STATE,ITEMSTD.ITEMID,
        ITEMSTD.INSPECTTYPE,ITEMSTD.LEVTWOSTD,ITEMSTD.LEVTHRSTD
    FROM 
        CH_PW_ETINSPECTTASK TASK,CH_PW_ETINSPECTTASKFB TASKFB,CH_PW_ETINSPECTTASKDET TASKDET,
        CH_PW_ETINSPECTITEMSTD ITEMSTD
    WHERE 
        TASK.TASKID = TASKDET.TASKID AND TASKDET.RECID = TASKFB.RECID AND TASK.VERSION = ITEMSTD.VERSION AND 
        TASKFB.ITEMID = ITEMSTD.ITEMID AND TASKFB.STATE = 6 AND TASK.REGION = 1 AND TO_CHAR(TASK.CREATEDATE,'YYYYMM') = '201701' AND 
        TASKDET.WAYID = '2') TASKINFO
LEFT JOIN TASKCHECKONE ON (TASKINFO.RECID = TASKCHECKONE.RECID AND TASKINFO.ITEMID = TASKCHECKONE.ITEMID)
LEFT JOIN TASKCHECKTWO ON (TASKINFO.RECID = TASKCHECKTWO.RECID AND TASKINFO.ITEMID = TASKCHECKTWO.ITEMID)
LEFT JOIN TASKCHECKFOUR ON (TASKINFO.RECID = TASKCHECKFOUR.RECID AND TASKINFO.ITEMID = TASKCHECKFOUR.ITEMID)
WHERE 1=1;

EXPLAIN (costs off) With BASE AS (
        SELECT taskdetail.TASKID,taskdetail.INFOID,infodetail.TITLE,infodetail.CONTENT,taskdetail.STATE,
            taskdetail.APPROVALLEVEL,
            (
                SELECT COUNT(*) FROM CH_TD_TASKDETAIL detail 
                WHERE detail.infoid =taskdetail.infoid AND detail.tasktype ='0' AND detail.state in ('0','1') AND 
                    ((detail.ISMULTIWAY=1) or (detail.ISMULTIWAY=0 AND detail.WAYID IS NULL))
            ) AS TOTALTASKCOUNT,
            (
                SELECT COUNT(*) FROM CH_TD_TASKDETAIL detail 
                WHERE detail.infoid =taskdetail.infoid and detail.tasktype ='0' and detail.state ='1' and 
                    ((detail.ISMULTIWAY=1) or (detail.ISMULTIWAY=0 AND detail.WAYID IS NULL))
            ) AS FINISHEDCOUNT,
            infodetail.OWNER,
            (
                select OPERNAME from SA_SYS_OPERATOR sysoper where sysoper.OPERID =infodetail.OWNER
            ) as OWNERNM,
            infodetail.PUBLISHDATE,taskdetail.ENDDATE AS TASKENDDATE,infodetail.ENDDATE AS INFOENDDATE,
            (
                SELECT COUNT(*) FROM CH_TD_INFOREPLYRECORD replyrecord WHERE replyrecord.infoid =taskdetail.infoid and replyrecord.REPLIEDID ='-1'
            ) AS REPLIEDIDCOUNT,
            (
                SELECT COUNT(*) FROM CH_TD_INFOATTACHMENTRELA attachmentrela WHERE attachmentrela.infoid =taskdetail.infoid
            ) AS ATTACHMENTCOUNT,
            taskdetail.WAYID,taskdetail.ISMULTIWAY,taskdetail.DISPATCHEDBY as DISPATCHEDID,
            (
                SELECT OPERNAME FROM SA_SYS_OPERATOR sysoper WHERE sysoper.OPERID =taskdetail.DISPATCHEDBY
            ) AS DISPATCHEDNM,
            (
                SELECT SOCIETYINFO.SOCIETYWAYNAME FROM SA_SYS_OPERATOR OPERATOR INNER JOIN CH_WAY_SOCIETYINFO SOCIETYINFO 
                ON SOCIETYINFO.SOCIETYWAYID = OPERATOR.ORGID WHERE OPERATOR.OPERID = taskdetail.DISPATCHEDBY
            ) AS DISPATCHEDSOCIETYWAYNM,
            taskdetail.UPDATEDATE as DISPATCHEDDATE,infodetail.infocolumn as INFOCOLUMNCD,
            (
                SELECT dict.dictname FROM SA_DB_DICTITEM dict WHERE dict.dictid =infodetail.infocolumn and dict.region =999 and dict.groupid='ColumnType'
            ) AS INFOCOLUMNNM,
            infodetail.REGION,
            (
                CASE WHEN infodetail.ENDDATE < sysdate THEN 1 ELSE 0 END
            ) AS PULLOFFFLAG,
            (
                SELECT SOCIETYINFO.SOCIETYWAYNAME FROM SA_SYS_OPERATOR OPERATOR INNER JOIN CH_WAY_SOCIETYINFO SOCIETYINFO 
                ON SOCIETYINFO.SOCIETYWAYID = OPERATOR.ORGID
                WHERE OPERATOR.OPERID = infodetail.OWNER
            ) AS SOCIETYWAYNAME,
            infodetail.CREATEDATE,
            (
                CASE WHEN (
                    SELECT COUNT(*) FROM CH_TD_MESSAGENOTICE mn WHERE mn.INFOID = infodetail.INFOID AND mn.ATTRID = taskdetail.TASKID AND mn.RECEIVER = taskdetail.RECEIVER AND
                        mn.STATE = 0 AND mn.NOTICETYPE = 1 ) >0 THEN 1 ELSE 0 END
            ) as URGEFLAG 
        FROM CH_TD_TASKDETAIL taskdetail INNER JOIN CH_TD_GENERALINFODETAIL infodetail ON taskdetail.infoid =infodetail.infoid Where taskdetail.RECEIVER = 2
            AND infodetail.state ='3' AND taskdetail.tasktype =0 AND taskdetail.state =0 AND taskdetail.issubtask =0 And infodetail.infotype ='task' AND 
            infodetail.MODULE NOT IN('xccx')
        order by infodetail.CREATEDATE desc)
SELECT TASKID,INFOID,TITLE,STATE,FLOOR(FINISHEDCOUNT/TOTALTASKCOUNT*100) FINISHEDRATE,FINISHEDCOUNT,TOTALTASKCOUNT,OWNER,OWNERNM,PUBLISHDATE,TASKENDDATE,INFOENDDATE,
    REPLIEDIDCOUNT,ATTACHMENTCOUNT,WAYID,ISMULTIWAY,DISPATCHEDID,DISPATCHEDNM,DISPATCHEDSOCIETYWAYNM,DISPATCHEDDATE,INFOCOLUMNCD,INFOCOLUMNNM,REGION,PULLOFFFLAG,
    SOCIETYWAYNAME,URGEFLAG
FROM BASE 
Where 1 =1
ORDER BY CREATEDATE desc
limit 10 offset 1;

EXPLAIN (costs off) With BASE AS (
        SELECT taskdetail.TASKID,taskdetail.INFOID,infodetail.TITLE,infodetail.CONTENT,taskdetail.STATE,
            taskdetail.APPROVALLEVEL,
            (
                SELECT COUNT(*) FROM CH_TD_TASKDETAIL detail 
                WHERE detail.infoid =taskdetail.infoid AND detail.tasktype ='0' AND detail.state in ('0','1') AND 
                    ((detail.ISMULTIWAY=1) or (detail.ISMULTIWAY=0 AND detail.WAYID IS NULL))
            ) AS TOTALTASKCOUNT,
            (
                SELECT COUNT(*) FROM CH_TD_TASKDETAIL detail 
                WHERE detail.infoid =taskdetail.infoid and detail.tasktype ='0' and detail.state ='1' and 
                    ((detail.ISMULTIWAY=1) or (detail.ISMULTIWAY=0 AND detail.WAYID IS NULL))
            ) AS FINISHEDCOUNT,
            infodetail.OWNER,
            (
                select OPERNAME from SA_SYS_OPERATOR sysoper where sysoper.OPERID =infodetail.OWNER
            ) as OWNERNM,
            infodetail.PUBLISHDATE,taskdetail.ENDDATE AS TASKENDDATE,infodetail.ENDDATE AS INFOENDDATE,
            (
                SELECT COUNT(*) FROM CH_TD_INFOREPLYRECORD replyrecord WHERE replyrecord.infoid =taskdetail.infoid and replyrecord.REPLIEDID ='-1'
            ) AS REPLIEDIDCOUNT,
            (
                SELECT COUNT(*) FROM CH_TD_INFOATTACHMENTRELA attachmentrela WHERE attachmentrela.infoid =taskdetail.infoid
            ) AS ATTACHMENTCOUNT,
            taskdetail.WAYID,taskdetail.ISMULTIWAY,taskdetail.DISPATCHEDBY as DISPATCHEDID,
            (
                SELECT OPERNAME FROM SA_SYS_OPERATOR sysoper WHERE sysoper.OPERID =taskdetail.DISPATCHEDBY
            ) AS DISPATCHEDNM,
            (
                SELECT SOCIETYINFO.SOCIETYWAYNAME FROM SA_SYS_OPERATOR OPERATOR INNER JOIN CH_WAY_SOCIETYINFO SOCIETYINFO 
                ON SOCIETYINFO.SOCIETYWAYID = OPERATOR.ORGID WHERE OPERATOR.OPERID = taskdetail.DISPATCHEDBY
            ) AS DISPATCHEDSOCIETYWAYNM,
            taskdetail.UPDATEDATE as DISPATCHEDDATE,infodetail.infocolumn as INFOCOLUMNCD,
            (
                SELECT dict.dictname FROM SA_DB_DICTITEM dict WHERE dict.dictid =infodetail.infocolumn and dict.region =999 and dict.groupid='ColumnType'
            ) AS INFOCOLUMNNM,
            infodetail.REGION,
            (
                CASE WHEN infodetail.ENDDATE < sysdate THEN 1 ELSE 0 END
            ) AS PULLOFFFLAG,
            (
                SELECT SOCIETYINFO.SOCIETYWAYNAME FROM SA_SYS_OPERATOR OPERATOR INNER JOIN CH_WAY_SOCIETYINFO SOCIETYINFO 
                ON SOCIETYINFO.SOCIETYWAYID = OPERATOR.ORGID
                WHERE OPERATOR.OPERID = infodetail.OWNER
            ) AS SOCIETYWAYNAME,
            infodetail.CREATEDATE,
            (
                CASE WHEN (
                    SELECT COUNT(*) FROM CH_TD_MESSAGENOTICE mn WHERE mn.INFOID = infodetail.INFOID AND mn.ATTRID = taskdetail.TASKID AND mn.RECEIVER = taskdetail.RECEIVER AND
                        mn.STATE = 0 AND mn.NOTICETYPE = 1 ) >0 THEN 1 ELSE 0 END
            ) as URGEFLAG 
        FROM CH_TD_TASKDETAIL taskdetail INNER JOIN CH_TD_GENERALINFODETAIL infodetail ON taskdetail.infoid =infodetail.infoid Where taskdetail.RECEIVER = 2
            AND infodetail.state ='3' AND taskdetail.tasktype =0 AND taskdetail.state =0 AND taskdetail.issubtask =0 And infodetail.infotype ='task' AND 
            infodetail.MODULE NOT IN('xccx')
        order by infodetail.CREATEDATE desc)
SELECT TASKID,INFOID,TITLE,STATE,FLOOR(FINISHEDCOUNT/TOTALTASKCOUNT*100) FINISHEDRATE,FINISHEDCOUNT,TOTALTASKCOUNT,OWNER,OWNERNM,PUBLISHDATE,TASKENDDATE,INFOENDDATE,
    REPLIEDIDCOUNT,ATTACHMENTCOUNT,WAYID,ISMULTIWAY,DISPATCHEDID,DISPATCHEDNM,DISPATCHEDSOCIETYWAYNM,DISPATCHEDDATE,INFOCOLUMNCD,INFOCOLUMNNM,REGION,PULLOFFFLAG,
    SOCIETYWAYNAME,URGEFLAG
FROM BASE 
Where 1 =1
ORDER BY CREATEDATE desc
limit 10 offset 1;

EXPLAIN (costs off) WITH NOTICEBASE AS(
        SELECT 
            GENERALINFODETAIL.INFOID,
            (
                SELECT DICT.DICTNAME FROM SA_DB_DICTITEM DICT WHERE DICT.DICTID = GENERALINFODETAIL.INFOCOLUMN AND 
                    DICT.REGION = 999 AND DICT.GROUPID='ColumnType'
            ) INFOCOLUMN,
            GENERALINFODETAIL.TITLE,GENERALINFODETAIL.PUBLISHDATE,GENERALINFODETAIL.OWNER OWNERID,
            (
                SELECT OPERATOR.OPERNAME FROM SA_SYS_OPERATOR OPERATOR WHERE OPERATOR.OPERID = GENERALINFODETAIL.OWNER
            ) OWNER1,
            (
                SELECT SOCIETYINFO.SOCIETYWAYNAME FROM CH_WAY_SOCIETYINFO SOCIETYINFO INNER JOIN SA_SYS_OPERATOR OPERATOR 
                    ON OPERATOR.ORGID = SOCIETYINFO.SOCIETYWAYID WHERE OPERATOR.OPERID = GENERALINFODETAIL.OWNER
            ) AS POSTNAME,
            (
                CASE WHEN GENERALINFODETAIL.INFOTYPE = 'notice' THEN (
                    SELECT COUNT(*) FROM CH_TD_INFORECEIVERRELA INFO_REVEIVE WHERE INFO_REVEIVE.INFOID = GENERALINFODETAIL.INFOID AND INFO_REVEIVE.ISREAD = '1') ELSE 0 END
            ) READCOUNT,
            (
                CASE WHEN GENERALINFODETAIL.INFOTYPE = 'notice' THEN(
                    SELECT COUNT(*)FROM CH_TD_INFORECEIVERRELA INFO_REVEIVE WHERE INFO_REVEIVE.INFOID = GENERALINFODETAIL.INFOID) ELSE 0 END
            ) TOTALREADCOUNT,
            0 FINISHEDCOUNT, 0 TOTALTASKCOUNT,
            (
                SELECT COUNT(*) FROM CH_TD_INFOREPLYRECORD REPLYRECORD WHERE REPLYRECORD.INFOID = GENERALINFODETAIL.INFOID AND REPLYRECORD.REPLIEDID = -1
            ) AS REPLYCOUNT,
            (
                SELECT COUNT(*) FROM CH_TD_INFOATTACHMENTRELA INFO_ATTACHMENT WHERE INFO_ATTACHMENT.INFOID = GENERALINFODETAIL.INFOID
            ) ATTACHMENTCOUNT,
            GENERALINFODETAIL.ENDDATE, GENERALINFODETAIL.INFOTYPE,0 TASKID,GENERALINFODETAIL.ISIMPORTANT,
            GENERALINFODETAIL.REGION,INFORECEIVERRELA.RECEIVER,GENERALINFODETAIL.CONTENT,INFORECEIVERRELA.UPDATEDATE,
            0 ISSUBTASK, 0 TASKDETAILSTATE, 0 TASKTYPE, 0 ISMULTIWAY,INFORECEIVERRELA.ISREAD,INFORECEIVERRELA.ISTOPRECORD
        FROM CH_TD_INFORECEIVERRELA INFORECEIVERRELA INNER JOIN CH_TD_GENERALINFODETAIL GENERALINFODETAIL 
        ON INFORECEIVERRELA.INFOID = GENERALINFODETAIL.INFOID
        WHERE GENERALINFODETAIL.STATE = '3' AND INFORECEIVERRELA.ISCOLLECTION = '1' AND INFORECEIVERRELA.RECEIVER = 1),
    TASKBASE AS(
        SELECT GENERALINFODETAIL.INFOID,
            (
                SELECT DICT.DICTNAME FROM SA_DB_DICTITEM DICT WHERE DICT.DICTID = GENERALINFODETAIL.INFOCOLUMN AND DICT.REGION = 999 AND DICT.GROUPID='ColumnType'
            ) INFOCOLUMN,
            GENERALINFODETAIL.TITLE,GENERALINFODETAIL.PUBLISHDATE,GENERALINFODETAIL.OWNER OWNERID,
            (
                SELECT OPERATOR.OPERNAME FROM SA_SYS_OPERATOR OPERATOR WHERE OPERATOR.OPERID = GENERALINFODETAIL.OWNER
            ) OWNER1,
            (
                SELECT SOCIETYINFO.SOCIETYWAYNAME FROM SA_SYS_OPERATOR OPERATOR INNER JOIN CH_WAY_SOCIETYINFO SOCIETYINFO 
                ON SOCIETYINFO.SOCIETYWAYID = OPERATOR.ORGID
                WHERE OPERATOR.OPERID = GENERALINFODETAIL.OWNER
            ) AS POSTNAME,
            0 READCOUNT,0 TOTALREADCOUNT,
            (
                CASE WHEN GENERALINFODETAIL.INFOTYPE = 'task' THEN(
                    SELECT COUNT(*) FROM CH_TD_TASKDETAIL DETAIL WHERE DETAIL.TASKID = TASKDETAIL.TASKID AND DETAIL.STATE = '1'AND 
                        DETAIL.TASKTYPE = 0 AND ((DETAIL.ISMULTIWAY = 1) OR (DETAIL.ISMULTIWAY = 0 AND DETAIL.WAYID IS NULL))) ELSE 0 END
            ) FINISHEDCOUNT,
            (
                CASE WHEN GENERALINFODETAIL.INFOTYPE = 'task' THEN(
                    SELECT COUNT(*)FROM CH_TD_TASKDETAIL DETAIL WHERE DETAIL.TASKID = TASKDETAIL.TASKID AND DETAIL.STATE IN ('0', '1') AND DETAIL.TASKTYPE = 0 AND
                        ((DETAIL.ISMULTIWAY = 1) OR(DETAIL.ISMULTIWAY = 0 AND DETAIL.WAYID IS NULL))) ELSE 0 END
            ) TOTALTASKCOUNT,
            (
                SELECT COUNT(*) FROM CH_TD_INFOREPLYRECORD REPLYRECORD WHERE REPLYRECORD.INFOID = GENERALINFODETAIL.INFOID AND REPLYRECORD.REPLIEDID = -1
            ) AS REPLYCOUNT,
            (
                SELECT COUNT(*)FROM CH_TD_INFOATTACHMENTRELA INFO_ATTACHMENT WHERE INFO_ATTACHMENT.INFOID = GENERALINFODETAIL.INFOID
            ) ATTACHMENTCOUNT,
            GENERALINFODETAIL.ENDDATE,GENERALINFODETAIL.INFOTYPE,TASKDETAIL.TASKID,GENERALINFODETAIL.ISIMPORTANT,GENERALINFODETAIL.REGION,
            TASKDETAIL.RECEIVER,GENERALINFODETAIL.CONTENT,TASKDETAIL.UPDATEDATE,TASKDETAIL.ISSUBTASK,TASKDETAIL.STATE AS TASKDETAILSTATE,
            TASKDETAIL.TASKTYPE,TASKDETAIL.ISMULTIWAY, 0 ISREAD,TASKDETAIL.ISTOPRECORD
        FROM CH_TD_TASKDETAIL TASKDETAIL INNER JOIN CH_TD_GENERALINFODETAIL GENERALINFODETAIL 
        ON TASKDETAIL.INFOID = GENERALINFODETAIL.INFOID
        WHERE GENERALINFODETAIL.STATE = '3'AND TASKDETAIL.ISCOLLECTION = '1'AND TASKDETAIL.RECEIVER = 1)
SELECT BASE.INFOID,BASE.INFOCOLUMN,BASE.TITLE,BASE.PUBLISHDATE,BASE.OWNERID,BASE.OWNER1,BASE.POSTNAME,BASE.READCOUNT,BASE.TOTALREADCOUNT,
    FLOOR(BASE.READCOUNT / BASE.TOTALREADCOUNT * 100) READRATE,BASE.FINISHEDCOUNT,BASE.TOTALTASKCOUNT,FLOOR(BASE.FINISHEDCOUNT / BASE.TOTALTASKCOUNT * 100) TASKRATE,
    BASE.REPLYCOUNT,BASE.ATTACHMENTCOUNT,BASE.ENDDATE,BASE.INFOTYPE,BASE.TASKID,BASE.ISIMPORTANT,BASE.REGION,BASE.RECEIVER,BASE.CONTENT,BASE.UPDATEDATE,
    BASE.ISSUBTASK,BASE.TASKDETAILSTATE,BASE.TASKTYPE,BASE.ISMULTIWAY,BASE.ISREAD,BASE.ISTOPRECORD
FROM (
    SELECT INFOID,INFOCOLUMN,TITLE,PUBLISHDATE,OWNERID,OWNER1,POSTNAME,READCOUNT,TOTALREADCOUNT,FINISHEDCOUNT,TOTALTASKCOUNT,REPLYCOUNT,ATTACHMENTCOUNT,ENDDATE,
        INFOTYPE,TASKID,ISIMPORTANT,REGION,RECEIVER,CONTENT,UPDATEDATE,ISSUBTASK,TASKDETAILSTATE,TASKTYPE,ISMULTIWAY,ISREAD,ISTOPRECORD
    FROM NOTICEBASE 
    UNION ALL 
    SELECT INFOID,INFOCOLUMN,TITLE,PUBLISHDATE,OWNERID,OWNER1,POSTNAME,READCOUNT,TOTALREADCOUNT,FINISHEDCOUNT,TOTALTASKCOUNT,REPLYCOUNT,ATTACHMENTCOUNT,ENDDATE,
        INFOTYPE,TASKID,ISIMPORTANT,REGION,RECEIVER,CONTENT,UPDATEDATE,ISSUBTASK,TASKDETAILSTATE,TASKTYPE,ISMULTIWAY,ISREAD,ISTOPRECORD
    FROM TASKBASE) BASE 
WHERE (BASE.TASKDETAILSTATE <> '2' OR BASE.TASKDETAILSTATE IS NULL)
ORDER BY BASE.ISTOPRECORD DESC,BASE.UPDATEDATE DESC
limit 10 offset 1;

EXPLAIN (costs off) WITH BASE AS (
        SELECT taskdetail.TASKID,taskdetail.INFOID,infodetail.TITLE,infodetail.CONTENT,taskdetail.STATE,
            taskdetail.APPROVALLEVEL,
            (
                SELECT COUNT(*) FROM CH_TD_TASKDETAIL detail 
                WHERE detail.infoid =taskdetail.infoid AND detail.tasktype ='0' AND detail.state IN ('0','1') AND
                    ((detail.ISMULTIWAY=1) OR (detail.ISMULTIWAY=0 AND detail.WAYID IS NULL))
            ) AS TOTALTASKCOUNT,
            (
                SELECT COUNT(*) FROM CH_TD_TASKDETAIL detail 
                WHERE detail.infoid =taskdetail.infoid AND detail.tasktype ='0' AND detail.state ='1' AND
                    ((detail.ISMULTIWAY=1) or (detail.ISMULTIWAY=0 AND detail.WAYID IS NULL))
            ) AS FINISHEDCOUNT,
            infodetail.OWNER,
            (
                SELECT OPERNAME FROM SA_SYS_OPERATOR sysoper WHERE sysoper.OPERID =infodetail.OWNER
            ) AS OWNERNM,
            infodetail.PUBLISHDATE,taskdetail.ENDDATE AS TASKENDDATE,infodetail.ENDDATE AS INFOENDDATE,
            (
                SELECT COUNT(*) FROM CH_TD_INFOREPLYRECORD replyrecord 
                WHERE replyrecord.infoid =taskdetail.infoid and replyrecord.REPLIEDID ='-1'
            ) AS REPLIEDIDCOUNT,
            (
                SELECT COUNT(*) FROM CH_TD_INFOATTACHMENTRELA attachmentrela 
                WHERE attachmentrela.infoid =taskdetail.infoid
            ) AS ATTACHMENTCOUNT,
            taskdetail.WAYID,taskdetail.ISMULTIWAY,taskdetail.DISPATCHEDBY as DISPATCHEDID,
            (
                SELECT OPERNAME FROM SA_SYS_OPERATOR sysoper WHERE sysoper.OPERID =taskdetail.DISPATCHEDBY
            ) as DISPATCHEDNM,
            (
                SELECT SOCIETYINFO.SOCIETYWAYNAME 
                FROM SA_SYS_OPERATOR OPERATOR INNER JOIN CH_WAY_SOCIETYINFO SOCIETYINFO 
                ON SOCIETYINFO.SOCIETYWAYID = OPERATOR.ORGID
                WHERE OPERATOR.OPERID = taskdetail.DISPATCHEDBY
            ) AS DISPATCHEDSOCIETYWAYNM,
            taskdetail.UPDATEDATE as DISPATCHEDDATE,infodetail.infocolumn as INFOCOLUMNCD,
            (
                select dict.dictname FROM SA_DB_DICTITEM dict 
                WHERE dict.dictid =infodetail.infocolumn AND dict.region =999 AND dict.groupid='ColumnType'
            ) AS INFOCOLUMNNM,
            infodetail.REGION,
            (case when infodetail.ENDDATE < sysdate then 1 else 0 end )as PULLOFFFLAG,
            (
                SELECT SOCIETYINFO.SOCIETYWAYNAME FROM SA_SYS_OPERATOR OPERATOR 
                INNER JOIN CH_WAY_SOCIETYINFO SOCIETYINFO ON SOCIETYINFO.SOCIETYWAYID = OPERATOR.ORGID
                WHERE OPERATOR.OPERID = infodetail.OWNER
            ) AS SOCIETYWAYNAME,
            infodetail.CREATEDATE,
            (case when (
                SELECT COUNT(*) FROM CH_TD_MESSAGENOTICE mn WHERE mn.INFOID = infodetail.INFOID AND 
                    mn.ATTRID = taskdetail.TASKID AND mn.RECEIVER = taskdetail.RECEIVER AND mn.STATE = 0 AND
                    mn.NOTICETYPE = 1 )>0 then 1 else 0 end
            ) as URGEFLAG
            FROM CH_TD_TASKDETAIL taskdetail INNER JOIN CH_TD_GENERALINFODETAIL infodetail 
            ON taskdetail.infoid =infodetail.infoid 
            WHERE taskdetail.RECEIVER =1 AND infodetail.state ='3' AND taskdetail.tasktype =0 AND 
                taskdetail.state =0 AND taskdetail.issubtask =0 AND infodetail.infotype ='task' AND
                infodetail.MODULE = 1
            order by infodetail.CREATEDATE desc)
SELECT TASKID,INFOID,TITLE,STATE,FLOOR(FINISHEDCOUNT/TOTALTASKCOUNT*100) FINISHEDRATE,
    FINISHEDCOUNT,TOTALTASKCOUNT,OWNER,OWNERNM,PUBLISHDATE,TASKENDDATE,INFOENDDATE,REPLIEDIDCOUNT,ATTACHMENTCOUNT,
    WAYID,ISMULTIWAY,DISPATCHEDID,DISPATCHEDNM,DISPATCHEDSOCIETYWAYNM,DISPATCHEDDATE,INFOCOLUMNCD,INFOCOLUMNNM,
    REGION,PULLOFFFLAG,SOCIETYWAYNAME,URGEFLAG 
FROM BASE 
WHERE 1 =1
ORDER BY CREATEDATE desc limit 10 offset 1;

EXPLAIN (costs off) WITH TASKCHECK AS(
        SELECT A.ITEMID,A.RECID,A.CHECKRESULT,A.COMPLAIN,B.CHRESULT,B.CHMEMO,B.OPRDATE
        FROM CH_PW_ETINSPECTCHECK A LEFT JOIN CH_PW_ETINSPECTCHECK B 
        ON (A.RECID = B.RECID AND A.ITEMID = B.ITEMID AND B.OPRTYPE = 3)
        WHERE A.OPRTYPE = 1)
SELECT 
    TASKINFO.TASKID,TASKINFO.RECID,TASKINFO.WAYID,TASKINFO.INSPECTDATE,TASKINFO.STARTINSPECTDATE,
    TASKINFO.ENDINSPECTDATE,TASKINFO.STAFFID,TASKINFO.INSPECTRESULT,TASKINFO.DEDUCT,TASKINFO.DEDUCTOBJ,
    TASKINFO.DEDUCTMEMO,TASKINFO.INSPECTMODE,TASKINFO.VERSION,TASKINFO.STATE,TASKINFO.ITEMID,
    TASKINFO.INSPECTTYPE,TASKINFO.LEVTWOSTD,TASKINFO.LEVTHRSTD,TASKINFO.OPRCODE,TASKCHECK.CHECKRESULT,
    TASKCHECK.COMPLAIN,RCD.RECORDCONTENT,TASKCHECK.CHRESULT,TASKCHECK.CHMEMO,TASKCHECK.OPRDATE,
    (
        SELECT COUNT(1) FROM CH_PW_ETINSPECTFBATTACH FBATTACH 
        WHERE 
            FBATTACH.TASKID = TASKINFO.TASKID AND FBATTACH.WAYID = TASKINFO.WAYID AND FBATTACH.OPERCODE = TASKINFO.OPRCODE AND 
            FBATTACH.ITEMID = TASKINFO.ITEMID AND FBATTACH.VERSION = TASKINFO.VERSION AND FBATTACH.ATTACHTYPE = 4
    ) ISATTACH,
    (
        SELECT NAME FROM SA_SYS_STAFFINFO WHERE STAFFID = TASKINFO.STAFFID
    ) SANAME,
    (
        SELECT SOCIETYWAYNAME FROM CH_WAY_SOCIETYINFO WHERE SOCIETYWAYID = TASKINFO.WAYID
    ) SOCIETYWAYNAME,
    (
        SELECT COUNT(1) FROM CH_PW_ETINSPECTCHECK C WHERE C.RECID = TASKINFO.RECID AND C.ITEMID = TASKINFO.ITEMID AND OPRTYPE = 3
    ) SCORESTD
    FROM (
        SELECT TASK.TASKID,TASK.VERSION,TASKDET.RECID,TASKDET.WAYID,TASKDET.STAFFID,TASKDET.INSPECTMODE,
            TASKFB.INSPECTDATE,TASKFB.STARTINSPECTDATE,TASKFB.ENDINSPECTDATE,TASKFB.INSPECTRESULT,TASKFB.DEDUCT,
            TASKFB.DEDUCTOBJ,TASKFB.DEDUCTMEMO,TASKFB.STATE,TASKFB.OPRCODE,ITEMSTD.ITEMID,ITEMSTD.INSPECTTYPE,
            ITEMSTD.LEVTWOSTD,ITEMSTD.LEVTHRSTD
        FROM CH_PW_ETINSPECTTASK TASK,CH_PW_ETINSPECTTASKFB TASKFB,CH_PW_ETINSPECTTASKDET TASKDET,CH_PW_ETINSPECTITEMSTD ITEMSTD
        WHERE 
            TASK.TASKID = TASKDET.TASKID AND TASKDET.RECID = TASKFB.RECID AND TASK.VERSION = ITEMSTD.VERSION AND
            TASKFB.ITEMID = ITEMSTD.ITEMID AND TASKFB.STATE = 4 AND TO_CHAR(TASK.CREATEDATE,'YYYYMM') = '201701'
        ) TASKINFO LEFT JOIN TASKCHECK ON (TASKINFO.RECID = TASKCHECK.RECID AND TASKINFO.ITEMID = TASKCHECK.ITEMID)
        LEFT JOIN CH_PW_ETINSPECTRCD RCD ON (RCD.VERSION = TASKINFO.VERSION AND RCD.WAYID = TASKINFO.WAYID AND RCD.ITEMID = TASKINFO.ITEMID)
WHERE 1 = 1;

--
-- cib cases
--
drop table if exists cib_zxkfxi_tbl;
create table cib_zxkfxi_tbl
( work_day timestamp(0) without time zone,
  zxkf_cnt numeric,
  xiaoi_session numeric
);
create index inx_zxkfxi_date on cib_zxkfxi_tbl(work_day);

drop table if exists cib_conn_log_tbl;
create table cib_conn_log_tbl
(
  id numeric(30,0) not null primary key,
  trade_begintime varchar(21),
  trade_endtime varchar(21),
  kh varchar(20),
  zh varchar(20),
  req_jydm  varchar(20),
  retcode  varchar(6)
);

create index idx_connn_log_kh on cib_conn_log_tbl(kh);
create index idx_connn_log_tradetime on cib_conn_log_tbl(trade_begintime,trade_endtime);
create index idx_connn_log_zh on cib_conn_log_tbl(zh);

create or replace function chr2date(datechar varchar)
returns timestamp without time zone
as $$
declare
    result timestamp(0);
begin
    result:=to_date(substr(datechar,1,17),'yyyymmdd hh24:mi:ss');
    return result;
exception
    when others then return null;
end;
$$ language default_plsql pushdown;


insert into cib_zxkfxi_tbl(work_day,zxkf_cnt,xiaoi_session)
select trunc(chr2date(t.trade_begintime))-1,
to_number(nvl(t.kh,0)) zxkf_cnt,
to_number(nvl(t.zh,0)) xiaoi_session
from cib_conn_log_tbl t
where t.trade_begintime=
(select max(a.trade_begintime) 
from cib_conn_log_tbl a
where a.req_jydm='301031'
and a.retcode='0'
and a.trade_begintime>=to_char(sysdate,'yyyymmdd')
and a.trade_begintime<to_char(sysdate+1,'yyyymmdd'));

drop table cib_zxkfxi_tbl cascade;
drop table cib_conn_log_tbl cascade;
drop function chr2date;

set enable_pullup_subquery TO off;
