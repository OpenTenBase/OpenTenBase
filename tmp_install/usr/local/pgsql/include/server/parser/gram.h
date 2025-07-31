/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_BASE_YY_GRAM_H_INCLUDED
# define YY_BASE_YY_GRAM_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int base_yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    IDENT = 258,                   /* IDENT  */
    FCONST = 259,                  /* FCONST  */
    SCONST = 260,                  /* SCONST  */
    BCONST = 261,                  /* BCONST  */
    XCONST = 262,                  /* XCONST  */
    Op = 263,                      /* Op  */
    ICONST = 264,                  /* ICONST  */
    PARAM = 265,                   /* PARAM  */
    TYPECAST = 266,                /* TYPECAST  */
    DOT_DOT = 267,                 /* DOT_DOT  */
    COLON_EQUALS = 268,            /* COLON_EQUALS  */
    EQUALS_GREATER = 269,          /* EQUALS_GREATER  */
    LESS_EQUALS = 270,             /* LESS_EQUALS  */
    GREATER_EQUALS = 271,          /* GREATER_EQUALS  */
    NOT_EQUALS = 272,              /* NOT_EQUALS  */
    ABORT_P = 273,                 /* ABORT_P  */
    ABSOLUTE_P = 274,              /* ABSOLUTE_P  */
    ACCESS = 275,                  /* ACCESS  */
    ACTION = 276,                  /* ACTION  */
    ADD_P = 277,                   /* ADD_P  */
    ADMIN = 278,                   /* ADMIN  */
    AFTER = 279,                   /* AFTER  */
    AGGREGATE = 280,               /* AGGREGATE  */
    ALL = 281,                     /* ALL  */
    ALSO = 282,                    /* ALSO  */
    ALTER = 283,                   /* ALTER  */
    ALWAYS = 284,                  /* ALWAYS  */
    ANALYSE = 285,                 /* ANALYSE  */
    ANALYZE = 286,                 /* ANALYZE  */
    AND = 287,                     /* AND  */
    ANY = 288,                     /* ANY  */
    ARRAY = 289,                   /* ARRAY  */
    AS = 290,                      /* AS  */
    ASC = 291,                     /* ASC  */
    ASSERTION = 292,               /* ASSERTION  */
    ASSIGNMENT = 293,              /* ASSIGNMENT  */
    ASYMMETRIC = 294,              /* ASYMMETRIC  */
    AT = 295,                      /* AT  */
    ATTACH = 296,                  /* ATTACH  */
    ATTRIBUTE = 297,               /* ATTRIBUTE  */
    AUDIT = 298,                   /* AUDIT  */
    AUTHORIZATION = 299,           /* AUTHORIZATION  */
    BACKWARD = 300,                /* BACKWARD  */
    BARRIER = 301,                 /* BARRIER  */
    BEFORE = 302,                  /* BEFORE  */
    BEGIN_P = 303,                 /* BEGIN_P  */
    BEGIN_SUBTXN = 304,            /* BEGIN_SUBTXN  */
    BETWEEN = 305,                 /* BETWEEN  */
    BIGINT = 306,                  /* BIGINT  */
    BINARY = 307,                  /* BINARY  */
    BINARY_FLOAT = 308,            /* BINARY_FLOAT  */
    BINARY_DOUBLE = 309,           /* BINARY_DOUBLE  */
    BIT = 310,                     /* BIT  */
    BOOLEAN_P = 311,               /* BOOLEAN_P  */
    BOTH = 312,                    /* BOTH  */
    BY = 313,                      /* BY  */
    CACHE = 314,                   /* CACHE  */
    CALLED = 315,                  /* CALLED  */
    CASCADE = 316,                 /* CASCADE  */
    CASCADED = 317,                /* CASCADED  */
    CASE = 318,                    /* CASE  */
    CAST = 319,                    /* CAST  */
    CATALOG_P = 320,               /* CATALOG_P  */
    CHAIN = 321,                   /* CHAIN  */
    CHAR_P = 322,                  /* CHAR_P  */
    CHARACTER = 323,               /* CHARACTER  */
    CHARACTERISTICS = 324,         /* CHARACTERISTICS  */
    CHECK = 325,                   /* CHECK  */
    CHECKPOINT = 326,              /* CHECKPOINT  */
    CLASS = 327,                   /* CLASS  */
    CLEAN = 328,                   /* CLEAN  */
    CLOSE = 329,                   /* CLOSE  */
    CLUSTER = 330,                 /* CLUSTER  */
    COALESCE = 331,                /* COALESCE  */
    COLLATE = 332,                 /* COLLATE  */
    COLLATION = 333,               /* COLLATION  */
    COLUMN = 334,                  /* COLUMN  */
    COLUMNS = 335,                 /* COLUMNS  */
    COMMENT = 336,                 /* COMMENT  */
    COMMENTS = 337,                /* COMMENTS  */
    COMMIT = 338,                  /* COMMIT  */
    COMMIT_SUBTXN = 339,           /* COMMIT_SUBTXN  */
    COMMITTED = 340,               /* COMMITTED  */
    CONCURRENTLY = 341,            /* CONCURRENTLY  */
    CONFIGURATION = 342,           /* CONFIGURATION  */
    CONFLICT = 343,                /* CONFLICT  */
    CONNECTION = 344,              /* CONNECTION  */
    CONSTRAINT = 345,              /* CONSTRAINT  */
    CONSTRAINTS = 346,             /* CONSTRAINTS  */
    CONTENT_P = 347,               /* CONTENT_P  */
    CONTINUE_P = 348,              /* CONTINUE_P  */
    CONVERSION_P = 349,            /* CONVERSION_P  */
    COORDINATOR = 350,             /* COORDINATOR  */
    COPY = 351,                    /* COPY  */
    COST = 352,                    /* COST  */
    CREATE = 353,                  /* CREATE  */
    CROSS = 354,                   /* CROSS  */
    CSV = 355,                     /* CSV  */
    CUBE = 356,                    /* CUBE  */
    CURRENT_P = 357,               /* CURRENT_P  */
    CURRENT_CATALOG = 358,         /* CURRENT_CATALOG  */
    CURRENT_DATE = 359,            /* CURRENT_DATE  */
    CURRENT_ROLE = 360,            /* CURRENT_ROLE  */
    CURRENT_SCHEMA = 361,          /* CURRENT_SCHEMA  */
    CURRENT_TIME = 362,            /* CURRENT_TIME  */
    CURRENT_TIMESTAMP = 363,       /* CURRENT_TIMESTAMP  */
    CURRENT_USER = 364,            /* CURRENT_USER  */
    CURSOR = 365,                  /* CURSOR  */
    CYCLE = 366,                   /* CYCLE  */
    DATA_P = 367,                  /* DATA_P  */
    DATABASE = 368,                /* DATABASE  */
    DAY_P = 369,                   /* DAY_P  */
    DBTIMEZONE = 370,              /* DBTIMEZONE  */
    DEALLOCATE = 371,              /* DEALLOCATE  */
    DEC = 372,                     /* DEC  */
    DECIMAL_P = 373,               /* DECIMAL_P  */
    DECLARE = 374,                 /* DECLARE  */
    DEFAULT = 375,                 /* DEFAULT  */
    DEFAULTS = 376,                /* DEFAULTS  */
    DEFERRABLE = 377,              /* DEFERRABLE  */
    DEFERRED = 378,                /* DEFERRED  */
    DEFINER = 379,                 /* DEFINER  */
    DELETE_P = 380,                /* DELETE_P  */
    DELIMITER = 381,               /* DELIMITER  */
    DELIMITERS = 382,              /* DELIMITERS  */
    DEPENDS = 383,                 /* DEPENDS  */
    DESC = 384,                    /* DESC  */
    DETACH = 385,                  /* DETACH  */
    DICTIONARY = 386,              /* DICTIONARY  */
    DIRECT = 387,                  /* DIRECT  */
    DISABLE_P = 388,               /* DISABLE_P  */
    DISCARD = 389,                 /* DISCARD  */
    DISTINCT = 390,                /* DISTINCT  */
    DISTKEY = 391,                 /* DISTKEY  */
    DISTRIBUTE = 392,              /* DISTRIBUTE  */
    DISTRIBUTED = 393,             /* DISTRIBUTED  */
    DISTSTYLE = 394,               /* DISTSTYLE  */
    DO = 395,                      /* DO  */
    DOCUMENT_P = 396,              /* DOCUMENT_P  */
    DOMAIN_P = 397,                /* DOMAIN_P  */
    DOUBLE_P = 398,                /* DOUBLE_P  */
    DROP = 399,                    /* DROP  */
    EACH = 400,                    /* EACH  */
    ELSE = 401,                    /* ELSE  */
    ENABLE_P = 402,                /* ENABLE_P  */
    ENCODING = 403,                /* ENCODING  */
    ENCRYPTED = 404,               /* ENCRYPTED  */
    END_P = 405,                   /* END_P  */
    ENUM_P = 406,                  /* ENUM_P  */
    ESCAPE = 407,                  /* ESCAPE  */
    EVENT = 408,                   /* EVENT  */
    EXCEPT = 409,                  /* EXCEPT  */
    EXCHANGE = 410,                /* EXCHANGE  */
    EXCLUDE = 411,                 /* EXCLUDE  */
    EXCLUDING = 412,               /* EXCLUDING  */
    EXCLUSIVE = 413,               /* EXCLUSIVE  */
    EXECUTE = 414,                 /* EXECUTE  */
    EXISTS = 415,                  /* EXISTS  */
    EXPLAIN = 416,                 /* EXPLAIN  */
    EXTENSION = 417,               /* EXTENSION  */
    EXTENT = 418,                  /* EXTENT  */
    EXTERNAL = 419,                /* EXTERNAL  */
    EXTRACT = 420,                 /* EXTRACT  */
    FALSE_P = 421,                 /* FALSE_P  */
    FAMILY = 422,                  /* FAMILY  */
    FETCH = 423,                   /* FETCH  */
    FILTER = 424,                  /* FILTER  */
    FIRST_P = 425,                 /* FIRST_P  */
    FLOAT_P = 426,                 /* FLOAT_P  */
    FOLLOWING = 427,               /* FOLLOWING  */
    FOR = 428,                     /* FOR  */
    FORCE = 429,                   /* FORCE  */
    FOREIGN = 430,                 /* FOREIGN  */
    FORWARD = 431,                 /* FORWARD  */
    FREEZE = 432,                  /* FREEZE  */
    FROM = 433,                    /* FROM  */
    FULL = 434,                    /* FULL  */
    FUNCTION = 435,                /* FUNCTION  */
    FUNCTIONS = 436,               /* FUNCTIONS  */
    GENERATED = 437,               /* GENERATED  */
    GLOBAL = 438,                  /* GLOBAL  */
    GRANT = 439,                   /* GRANT  */
    GRANTED = 440,                 /* GRANTED  */
    GREATEST = 441,                /* GREATEST  */
    GROUP_P = 442,                 /* GROUP_P  */
    GROUPING = 443,                /* GROUPING  */
    GTM = 444,                     /* GTM  */
    HANDLER = 445,                 /* HANDLER  */
    HAVING = 446,                  /* HAVING  */
    HEADER_P = 447,                /* HEADER_P  */
    HOLD = 448,                    /* HOLD  */
    HOUR_P = 449,                  /* HOUR_P  */
    IDENTITY_P = 450,              /* IDENTITY_P  */
    IF_P = 451,                    /* IF_P  */
    ILIKE = 452,                   /* ILIKE  */
    IMMEDIATE = 453,               /* IMMEDIATE  */
    IMMUTABLE = 454,               /* IMMUTABLE  */
    IMPLICIT_P = 455,              /* IMPLICIT_P  */
    IMPORT_P = 456,                /* IMPORT_P  */
    IN_P = 457,                    /* IN_P  */
    INCLUDING = 458,               /* INCLUDING  */
    INCREMENT = 459,               /* INCREMENT  */
    INDEX = 460,                   /* INDEX  */
    INDEXES = 461,                 /* INDEXES  */
    INHERIT = 462,                 /* INHERIT  */
    INHERITS = 463,                /* INHERITS  */
    INITIALLY = 464,               /* INITIALLY  */
    INLINE_P = 465,                /* INLINE_P  */
    INNER_P = 466,                 /* INNER_P  */
    INOUT = 467,                   /* INOUT  */
    INPUT_P = 468,                 /* INPUT_P  */
    INSENSITIVE = 469,             /* INSENSITIVE  */
    INSERT = 470,                  /* INSERT  */
    INSTEAD = 471,                 /* INSTEAD  */
    INT_P = 472,                   /* INT_P  */
    INTEGER = 473,                 /* INTEGER  */
    INTERSECT = 474,               /* INTERSECT  */
    INTERVAL = 475,                /* INTERVAL  */
    INTO = 476,                    /* INTO  */
    INVOKER = 477,                 /* INVOKER  */
    IS = 478,                      /* IS  */
    ISNULL = 479,                  /* ISNULL  */
    ISOLATION = 480,               /* ISOLATION  */
    JOIN = 481,                    /* JOIN  */
    KEY = 482,                     /* KEY  */
    LABEL = 483,                   /* LABEL  */
    LANGUAGE = 484,                /* LANGUAGE  */
    LARGE_P = 485,                 /* LARGE_P  */
    LAST_P = 486,                  /* LAST_P  */
    LATERAL_P = 487,               /* LATERAL_P  */
    LEADING = 488,                 /* LEADING  */
    LEAKPROOF = 489,               /* LEAKPROOF  */
    LEAST = 490,                   /* LEAST  */
    LEFT = 491,                    /* LEFT  */
    LEVEL = 492,                   /* LEVEL  */
    LIKE = 493,                    /* LIKE  */
    LIMIT = 494,                   /* LIMIT  */
    LISTEN = 495,                  /* LISTEN  */
    LOAD = 496,                    /* LOAD  */
    LOCAL = 497,                   /* LOCAL  */
    LOCALTIME = 498,               /* LOCALTIME  */
    LOCALTIMESTAMP = 499,          /* LOCALTIMESTAMP  */
    LOCATION = 500,                /* LOCATION  */
    LOCK_P = 501,                  /* LOCK_P  */
    LOCKED = 502,                  /* LOCKED  */
    LOGGED = 503,                  /* LOGGED  */
    MAPPING = 504,                 /* MAPPING  */
    MATCH = 505,                   /* MATCH  */
    MATERIALIZED = 506,            /* MATERIALIZED  */
    MAXVALUE = 507,                /* MAXVALUE  */
    METHOD = 508,                  /* METHOD  */
    MINUTE_P = 509,                /* MINUTE_P  */
    MINVALUE = 510,                /* MINVALUE  */
    MODE = 511,                    /* MODE  */
    MONTH_P = 512,                 /* MONTH_P  */
    MOVE = 513,                    /* MOVE  */
    NAME_P = 514,                  /* NAME_P  */
    NAMES = 515,                   /* NAMES  */
    NATIONAL = 516,                /* NATIONAL  */
    NATURAL = 517,                 /* NATURAL  */
    NCHAR = 518,                   /* NCHAR  */
    NEW = 519,                     /* NEW  */
    NEXT = 520,                    /* NEXT  */
    NO = 521,                      /* NO  */
    NOAUDIT = 522,                 /* NOAUDIT  */
    NODE = 523,                    /* NODE  */
    NONE = 524,                    /* NONE  */
    NOT = 525,                     /* NOT  */
    NOTHING = 526,                 /* NOTHING  */
    NOTIFY = 527,                  /* NOTIFY  */
    NOTNULL = 528,                 /* NOTNULL  */
    NOWAIT = 529,                  /* NOWAIT  */
    NULL_P = 530,                  /* NULL_P  */
    NULLIF = 531,                  /* NULLIF  */
    NULLS_P = 532,                 /* NULLS_P  */
    NUMBER = 533,                  /* NUMBER  */
    NUMERIC = 534,                 /* NUMERIC  */
    OBJECT_P = 535,                /* OBJECT_P  */
    OF = 536,                      /* OF  */
    OFF = 537,                     /* OFF  */
    OFFSET = 538,                  /* OFFSET  */
    OID_P = 539,                   /* OID_P  */
    OIDS = 540,                    /* OIDS  */
    OLD = 541,                     /* OLD  */
    ON = 542,                      /* ON  */
    ONLY = 543,                    /* ONLY  */
    OPENTENBASE_P = 544,           /* OPENTENBASE_P  */
    OPERATOR = 545,                /* OPERATOR  */
    OPTION = 546,                  /* OPTION  */
    OPTIONS = 547,                 /* OPTIONS  */
    OR = 548,                      /* OR  */
    ORDER = 549,                   /* ORDER  */
    ORDINALITY = 550,              /* ORDINALITY  */
    OUT_P = 551,                   /* OUT_P  */
    OUTER_P = 552,                 /* OUTER_P  */
    OVER = 553,                    /* OVER  */
    OVERLAPS = 554,                /* OVERLAPS  */
    OVERLAY = 555,                 /* OVERLAY  */
    OVERRIDING = 556,              /* OVERRIDING  */
    OWNED = 557,                   /* OWNED  */
    OWNER = 558,                   /* OWNER  */
    PARALLEL = 559,                /* PARALLEL  */
    PARSER = 560,                  /* PARSER  */
    PARTIAL = 561,                 /* PARTIAL  */
    PARTITION = 562,               /* PARTITION  */
    PARTITIONS = 563,              /* PARTITIONS  */
    PASSING = 564,                 /* PASSING  */
    PASSWORD = 565,                /* PASSWORD  */
    PAUSE = 566,                   /* PAUSE  */
    PLACING = 567,                 /* PLACING  */
    PLANS = 568,                   /* PLANS  */
    POLICY = 569,                  /* POLICY  */
    POSITION = 570,                /* POSITION  */
    PRECEDING = 571,               /* PRECEDING  */
    PRECISION = 572,               /* PRECISION  */
    PREFERRED = 573,               /* PREFERRED  */
    PRESERVE = 574,                /* PRESERVE  */
    PREPARE = 575,                 /* PREPARE  */
    PREPARED = 576,                /* PREPARED  */
    PRIMARY = 577,                 /* PRIMARY  */
    PRIOR = 578,                   /* PRIOR  */
    PRIVILEGES = 579,              /* PRIVILEGES  */
    PROCEDURAL = 580,              /* PROCEDURAL  */
    PROCEDURE = 581,               /* PROCEDURE  */
    PROGRAM = 582,                 /* PROGRAM  */
    PUBLICATION = 583,             /* PUBLICATION  */
    PUSHDOWN = 584,                /* PUSHDOWN  */
    QUOTE = 585,                   /* QUOTE  */
    RANDOMLY = 586,                /* RANDOMLY  */
    RANGE = 587,                   /* RANGE  */
    READ = 588,                    /* READ  */
    REAL = 589,                    /* REAL  */
    REASSIGN = 590,                /* REASSIGN  */
    REBUILD = 591,                 /* REBUILD  */
    RECHECK = 592,                 /* RECHECK  */
    RECURSIVE = 593,               /* RECURSIVE  */
    REF = 594,                     /* REF  */
    REFERENCES = 595,              /* REFERENCES  */
    REFERENCING = 596,             /* REFERENCING  */
    REFRESH = 597,                 /* REFRESH  */
    REINDEX = 598,                 /* REINDEX  */
    RELATIVE_P = 599,              /* RELATIVE_P  */
    RELEASE = 600,                 /* RELEASE  */
    RENAME = 601,                  /* RENAME  */
    REPEATABLE = 602,              /* REPEATABLE  */
    REPLACE = 603,                 /* REPLACE  */
    REPLICA = 604,                 /* REPLICA  */
    RESET = 605,                   /* RESET  */
    RESTART = 606,                 /* RESTART  */
    RESTRICT = 607,                /* RESTRICT  */
    RETURNING = 608,               /* RETURNING  */
    RETURNS = 609,                 /* RETURNS  */
    REVOKE = 610,                  /* REVOKE  */
    RIGHT = 611,                   /* RIGHT  */
    ROLE = 612,                    /* ROLE  */
    ROLLBACK = 613,                /* ROLLBACK  */
    ROLLBACK_SUBTXN = 614,         /* ROLLBACK_SUBTXN  */
    ROLLUP = 615,                  /* ROLLUP  */
    ROW = 616,                     /* ROW  */
    ROWS = 617,                    /* ROWS  */
    RULE = 618,                    /* RULE  */
    SAMPLE = 619,                  /* SAMPLE  */
    SAVEPOINT = 620,               /* SAVEPOINT  */
    SCHEMA = 621,                  /* SCHEMA  */
    SCHEMAS = 622,                 /* SCHEMAS  */
    SCROLL = 623,                  /* SCROLL  */
    SEARCH = 624,                  /* SEARCH  */
    SECOND_P = 625,                /* SECOND_P  */
    SECURITY = 626,                /* SECURITY  */
    SELECT = 627,                  /* SELECT  */
    SEQUENCE = 628,                /* SEQUENCE  */
    SEQUENCES = 629,               /* SEQUENCES  */
    SERIALIZABLE = 630,            /* SERIALIZABLE  */
    SERVER = 631,                  /* SERVER  */
    SESSION = 632,                 /* SESSION  */
    SESSION_USER = 633,            /* SESSION_USER  */
    SESSIONTIMEZONE = 634,         /* SESSIONTIMEZONE  */
    SET = 635,                     /* SET  */
    SETS = 636,                    /* SETS  */
    SETOF = 637,                   /* SETOF  */
    SHARDING = 638,                /* SHARDING  */
    SHARE = 639,                   /* SHARE  */
    SHOW = 640,                    /* SHOW  */
    SIMILAR = 641,                 /* SIMILAR  */
    SIMPLE = 642,                  /* SIMPLE  */
    SKIP = 643,                    /* SKIP  */
    SLOT = 644,                    /* SLOT  */
    SMALLINT = 645,                /* SMALLINT  */
    SNAPSHOT = 646,                /* SNAPSHOT  */
    SOME = 647,                    /* SOME  */
    SQL_P = 648,                   /* SQL_P  */
    STABLE = 649,                  /* STABLE  */
    STANDALONE_P = 650,            /* STANDALONE_P  */
    START = 651,                   /* START  */
    STATEMENT = 652,               /* STATEMENT  */
    STATISTICS = 653,              /* STATISTICS  */
    STDIN = 654,                   /* STDIN  */
    STDOUT = 655,                  /* STDOUT  */
    STEP = 656,                    /* STEP  */
    STORAGE = 657,                 /* STORAGE  */
    STRICT_P = 658,                /* STRICT_P  */
    STRIP_P = 659,                 /* STRIP_P  */
    SUBSCRIPTION = 660,            /* SUBSCRIPTION  */
    SUBSTRING = 661,               /* SUBSTRING  */
    SUCCESSFUL = 662,              /* SUCCESSFUL  */
    SYMMETRIC = 663,               /* SYMMETRIC  */
    SYNC = 664,                    /* SYNC  */
    SYSDATE = 665,                 /* SYSDATE  */
    SYSID = 666,                   /* SYSID  */
    SYSTEM_P = 667,                /* SYSTEM_P  */
    SYSTIMESTAMP = 668,            /* SYSTIMESTAMP  */
    TABLE = 669,                   /* TABLE  */
    TABLES = 670,                  /* TABLES  */
    TABLESAMPLE = 671,             /* TABLESAMPLE  */
    TABLESPACE = 672,              /* TABLESPACE  */
    TEMP = 673,                    /* TEMP  */
    TEMPLATE = 674,                /* TEMPLATE  */
    TEMPORARY = 675,               /* TEMPORARY  */
    TEXT_P = 676,                  /* TEXT_P  */
    THEN = 677,                    /* THEN  */
    TIME = 678,                    /* TIME  */
    TIMESTAMP = 679,               /* TIMESTAMP  */
    TO = 680,                      /* TO  */
    TRAILING = 681,                /* TRAILING  */
    TRANSACTION = 682,             /* TRANSACTION  */
    TRANSFORM = 683,               /* TRANSFORM  */
    TREAT = 684,                   /* TREAT  */
    TRIGGER = 685,                 /* TRIGGER  */
    TRIM = 686,                    /* TRIM  */
    TRUE_P = 687,                  /* TRUE_P  */
    TRUNCATE = 688,                /* TRUNCATE  */
    TRUSTED = 689,                 /* TRUSTED  */
    TYPE_P = 690,                  /* TYPE_P  */
    TYPES_P = 691,                 /* TYPES_P  */
    UNBOUNDED = 692,               /* UNBOUNDED  */
    UNCOMMITTED = 693,             /* UNCOMMITTED  */
    UNENCRYPTED = 694,             /* UNENCRYPTED  */
    UNION = 695,                   /* UNION  */
    UNIQUE = 696,                  /* UNIQUE  */
    UNKNOWN = 697,                 /* UNKNOWN  */
    UNLISTEN = 698,                /* UNLISTEN  */
    UNLOCK_P = 699,                /* UNLOCK_P  */
    UNLOGGED = 700,                /* UNLOGGED  */
    UNPAUSE = 701,                 /* UNPAUSE  */
    UNTIL = 702,                   /* UNTIL  */
    UPDATE = 703,                  /* UPDATE  */
    USER = 704,                    /* USER  */
    USING = 705,                   /* USING  */
    VACUUM = 706,                  /* VACUUM  */
    VALID = 707,                   /* VALID  */
    VALIDATE = 708,                /* VALIDATE  */
    VALIDATOR = 709,               /* VALIDATOR  */
    VALUE_P = 710,                 /* VALUE_P  */
    VALUES = 711,                  /* VALUES  */
    VARCHAR = 712,                 /* VARCHAR  */
    VARIADIC = 713,                /* VARIADIC  */
    VARYING = 714,                 /* VARYING  */
    VERBOSE = 715,                 /* VERBOSE  */
    VERSION_P = 716,               /* VERSION_P  */
    VIEW = 717,                    /* VIEW  */
    VIEWS = 718,                   /* VIEWS  */
    VOLATILE = 719,                /* VOLATILE  */
    WHEN = 720,                    /* WHEN  */
    WHENEVER = 721,                /* WHENEVER  */
    WHERE = 722,                   /* WHERE  */
    WHITESPACE_P = 723,            /* WHITESPACE_P  */
    WINDOW = 724,                  /* WINDOW  */
    WITH = 725,                    /* WITH  */
    WITHIN = 726,                  /* WITHIN  */
    WITHOUT = 727,                 /* WITHOUT  */
    WORK = 728,                    /* WORK  */
    WRAPPER = 729,                 /* WRAPPER  */
    WRITE = 730,                   /* WRITE  */
    XML_P = 731,                   /* XML_P  */
    XMLATTRIBUTES = 732,           /* XMLATTRIBUTES  */
    XMLCONCAT = 733,               /* XMLCONCAT  */
    XMLELEMENT = 734,              /* XMLELEMENT  */
    XMLEXISTS = 735,               /* XMLEXISTS  */
    XMLFOREST = 736,               /* XMLFOREST  */
    XMLNAMESPACES = 737,           /* XMLNAMESPACES  */
    XMLPARSE = 738,                /* XMLPARSE  */
    XMLPI = 739,                   /* XMLPI  */
    XMLROOT = 740,                 /* XMLROOT  */
    XMLSERIALIZE = 741,            /* XMLSERIALIZE  */
    XMLTABLE = 742,                /* XMLTABLE  */
    YEAR_P = 743,                  /* YEAR_P  */
    YES_P = 744,                   /* YES_P  */
    ZONE = 745,                    /* ZONE  */
    NOT_LA = 746,                  /* NOT_LA  */
    NULLS_LA = 747,                /* NULLS_LA  */
    WITH_LA = 748,                 /* WITH_LA  */
    POSTFIXOP = 749,               /* POSTFIXOP  */
    UMINUS = 750                   /* UMINUS  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 219 "gram.y"

	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;

	char				chr;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
	OnCommitAction		oncommit;
	List				*list;
	Node				*node;
	Value				*value;
	ObjectType			objtype;
	TypeName			*typnam;
	FunctionParameter   *fun_param;
	FunctionParameterMode fun_param_mode;
	ObjectWithArgs		*objwithargs;
	DefElem				*defelt;
	SortBy				*sortby;
	WindowDef			*windef;
	JoinExpr			*jexpr;
	IndexElem			*ielem;
	Alias				*alias;
	RangeVar			*range;
	IntoClause			*into;
	WithClause			*with;
	InferClause			*infer;
	OnConflictClause	*onconflict;
	A_Indices			*aind;
	ResTarget			*target;
	struct PrivTarget	*privtarget;
	AccessPriv			*accesspriv;
	struct ImportQual	*importqual;
	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;
/* PGXC_BEGIN */
	DistributeBy		*distby;
	PGXCSubCluster		*subclus;
/* PGXC_END */
	A_Const				*a_const;
	PartitionElem		*partelem;
	PartitionSpec		*partspec;
	PartitionBoundSpec	*partboundspec;
	RoleSpec			*rolespec;
	PartitionForExpr	*partfor;
	PartitionBy         *partby; 
	StatSyncOpt      *analyze_sync_opt;

#line 611 "gram.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif




int base_yyparse (core_yyscan_t yyscanner);


#endif /* !YY_BASE_YY_GRAM_H_INCLUDED  */
