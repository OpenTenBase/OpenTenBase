/*COMP9315项目：创建一个新的数据类型EmailAddr

*/

#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "libpq/pqformat.h"        /*发送/接收功能所需*/
#include <ctype.h>

PG_MODULE_MAGIC;

typedef struct EmailAddr {
    int32 length;
    char email[FLEXIBLE_ARRAY_MEMBER];
} EmailAddr;


//功能声明
int checkIfValidEmail(char *, char, char *);
void errorDumper(char *);
int intermediate(char *, char *, int);
int hashFn(char *);
void lenchecker(int, char *);

//检查输入字符串的长度是否大于256
//按照RFC822标准
void lenchecker(int i, char *str) {
    if (i > 256) {
        errorDumper(str);
    }
}

//作为第一个字符的散列函数的逻辑决定了我们将函数存储在哪个桶中
//26个桶之一的可能输入
int hashFn(char *str) {
    return ((int) (*str));
}

//打印出错误消息
void errorDumper(char *e) {
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for EmailAddr: \"%s\"",
                           e)));
}

//通过对电子邮件地址进行各种测试来检查其是否有效
//根据ASCII字符值进行比较
//自己实施

int checkIfValidEmail(char *let, char sym, char *org) {
    int count2 = strlen(let);
    int i = 1, countAt = 0;
    bool nextFlag = false;
    
    if (!((*let >= 97 && *let <= 122) || (*let >= 65 && *let <= 90)))
        errorDumper(org);

    if (count2 < 2) {
        return countAt;
    }

    let += 1;

    while (i < count2 - 1) {
        if (nextFlag == true) {

            if (!((*let >= 97 && *let <= 122) || (*let >= 65 && *let <= 90) || *let == '@')) {
                errorDumper(org);
            }
            nextFlag = false;
        }


        if (!((*let >= 97 && *let <= 122) || (*let >= 65 && *let <= 90) || (*let >= 48 && *let <= 57) || *let == 45 ||
              *let == 46 || *let == '@')) {
            errorDumper(org);
        }

        if (*let == sym) {
            countAt++;
        }

        if (*let == 46) {
            nextFlag = true;
        }

        i++;
        let++;
    }

    if (!((*let >= 97 && *let <= 122) || (*let >= 65 && *let <= 90) || (*let >= 48 && *let <= 57))) {
        errorDumper(org);
    }

    return countAt;
}

//用于词汇比较的字符串比较函数
static int email_abs_cmp_internal(char *a, char *b) {
    if (strcmp(a, b) > 0)  // 1 > 2
        return 1;
    else if (strcmp(a, b) < 0)  // 1 < 2
        return -1;
    return 0;  // 1 = 2
}

//用于字符串比较的中间函数
int intermediate(char *str1, char *str2, int op) {
    int32 len11, len12, len13;
    int32 len21, len22, len23;
    char *local1, *domain1, *splitter1;
    char *local2, *domain2, *splitter2;
    char *delim = "@";
    char *temp1, *temp2;

    len11 = strlen(str1);
    temp1 = (char *) palloc((len11 + 1) * sizeof(char));
    strcpy(temp1, str1);
    splitter1 = (char *) palloc((len11 + 1) * sizeof(char));
    splitter1 = strtok(temp1, delim);

    len12 = strlen(splitter1);
    local1 = (char *) palloc((len12 + 1) * sizeof(char));
    strcpy(local1, splitter1);
    
    splitter1 = strtok(NULL, delim);
    len13 = strlen(splitter1);
    domain1 = (char *) palloc((len13 + 1) * sizeof(char));
    strcpy(domain1, splitter1);

    len21 = strlen(str2);
    temp2 = (char *) palloc((len21 + 1) * sizeof(char));
    strcpy(temp2, str2);
    splitter2 = (char *) palloc((len21 + 1) * sizeof(char));
    splitter2 = strtok(temp2, delim);

    len22 = strlen(splitter2);
    local2 = (char *) palloc((len22 + 1) * sizeof(char));
    strcpy(local2, splitter2);
    
    splitter2 = strtok(NULL, delim);
    len23 = strlen(splitter2);
    domain2 = (char *) palloc((len23 + 1) * sizeof(char));
    strcpy(domain2, splitter2);

    if (op == 3) {
        if (email_abs_cmp_internal(domain1, domain2) == 0 && email_abs_cmp_internal(local1, local2) == 0) {
            return 0;
        } else { return -1; }

    } else if (op == 6) {
        if (email_abs_cmp_internal(domain1, domain2) == 1) {
            return 1;
        } else if (email_abs_cmp_internal(domain1, domain2) == 0 && (email_abs_cmp_internal(local1, local2) == 1)) {
            return 1;
        } else { return 0; }

    } else if (op == 5) {
        if (email_abs_cmp_internal(domain1, domain2) == 1) {
            return 1;
        } else if (email_abs_cmp_internal(domain1, domain2) == 0 && (email_abs_cmp_internal(local1, local2) == 0)) {
            return 1;
        } else { return 0; }

    } else if (op == 7) {
        if (email_abs_cmp_internal(domain1, domain2) == 0) {
            return 0;
        } else { return -1; }

    } else if (op == 1) {
        if (email_abs_cmp_internal(domain1, domain2) == -1) {
            return -1;
        } else if (email_abs_cmp_internal(domain1, domain2) == 0 && (email_abs_cmp_internal(local1, local2) == -1)) {
            return -1;
        } else { return 0; }

    } else if (op == 2) {
        if (email_abs_cmp_internal(domain1, domain2) == -1) {
            return -1;
        } else if (email_abs_cmp_internal(domain1, domain2) == 0 && (email_abs_cmp_internal(local1, local2) == 0)) {
            return -1;
        } else { return 0; }
    } else {
        if (email_abs_cmp_internal(domain1, domain2) != 0) {
            return email_abs_cmp_internal(domain1, domain2);
        } else {
            return email_abs_cmp_internal(local1, local2);
        }
    }
}

/*****************************************************************************
 * Input/Output 
 *****************************************************************************/

PG_FUNCTION_INFO_V1(email_in);

Datum
email_in(PG_FUNCTION_ARGS) {
    int i;
    char *delim = "@";
    EmailAddr *result;
    int32 len1, len2, len3, len4;
    char *str = PG_GETARG_CSTRING(0);
    char *local, *domain, *splitter, *email;
    
    len1 = strlen(str);


    if (checkIfValidEmail(str, '@', str) != 1) {
        errorDumper(str);
    }

    for (i = 0; str[i]; i++) {
        str[i] = tolower(str[i]);
    }

    //将电子邮件拆分为本地和域部分，并调用checkIfValidEmail
    //运行测试
    splitter = (char *) palloc((len1 + 1) * sizeof(char));
    splitter = strtok(str, delim);

    len2 = strlen(splitter);
    local = (char *) palloc((len2 + 1) * sizeof(char));
    lenchecker(len2, str);
    strcpy(local, splitter);

    splitter = strtok(NULL, delim);
    len3 = strlen(splitter);
    lenchecker(len3, str);
    domain = (char *) palloc((len3 + 1) * sizeof(char));
    strcpy(domain, splitter);

    checkIfValidEmail(local, ',', str);

    if (checkIfValidEmail(domain, '.', str) < 1) {
        errorDumper(str);
    }
    
    //如果所有测试都通过了，那么重新生成字符串并为
    //将其存储到数据库中
    email = (char *) palloc((len2 + len3 + VARHDRSZ + 1) * sizeof(char));
    strcpy(email, local);
    strcat(email, "@");
    strcat(email, domain);

    len4 = strlen(email);

    result = (EmailAddr *) palloc(VARHDRSZ + len4 + 1);
    SET_VARSIZE(result, VARHDRSZ + len4 + 4);   
    memcpy(result->email, email, len4 + 1);
 
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(email_out);

Datum
email_out(PG_FUNCTION_ARGS) {
    EmailAddr *emailAddr = (EmailAddr *) PG_GETARG_POINTER(0);
    char *result;

    result = psprintf("%s", emailAddr->email);
    PG_RETURN_CSTRING(result);
}




PG_FUNCTION_INFO_V1(email_abs_lt);

Datum
email_abs_lt(PG_FUNCTION_ARGS) {
    EmailAddr *emailAddr1 = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr *emailAddr2 = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(intermediate(emailAddr1->email, emailAddr2->email, 1) == -1);
}

PG_FUNCTION_INFO_V1(email_abs_le);

Datum
email_abs_le(PG_FUNCTION_ARGS) {
    EmailAddr *emailAddr1 = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr *emailAddr2 = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(intermediate(emailAddr1->email, emailAddr2->email, 2) == -1);
}

PG_FUNCTION_INFO_V1(email_abs_eq);

Datum
email_abs_eq(PG_FUNCTION_ARGS) {
    EmailAddr *emailAddr1 = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr *emailAddr2 = (EmailAddr *) PG_GETARG_POINTER(1);
    PG_RETURN_BOOL(intermediate(emailAddr1->email, emailAddr2->email, 3) == 0);

}

PG_FUNCTION_INFO_V1(email_abs_ge);

Datum
email_abs_ge(PG_FUNCTION_ARGS) {
    EmailAddr *emailAddr1 = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr *emailAddr2 = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(intermediate(emailAddr1->email, emailAddr2->email, 5) == 1);
}

PG_FUNCTION_INFO_V1(email_abs_gt);

Datum
email_abs_gt(PG_FUNCTION_ARGS) {
    EmailAddr *emailAddr1 = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr *emailAddr2 = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(intermediate(emailAddr1->email, emailAddr2->email, 6) == 1);
}

PG_FUNCTION_INFO_V1(email_abs_lg);

Datum
email_abs_lg(PG_FUNCTION_ARGS) {
    EmailAddr *emailAddr1 = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr *emailAddr2 = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(intermediate(emailAddr1->email, emailAddr2->email, 3) != 0);
}

PG_FUNCTION_INFO_V1(email_abs_tdeq);

Datum
email_abs_tdeq(PG_FUNCTION_ARGS) {
    EmailAddr *emailAddr1 = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr *emailAddr2 = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(intermediate(emailAddr1->email, emailAddr2->email, 7) == 0);
}

PG_FUNCTION_INFO_V1(email_abs_tdnteq);

Datum
email_abs_tdnteq(PG_FUNCTION_ARGS) {
    EmailAddr *emailAddr1 = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr *emailAddr2 = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(intermediate(emailAddr1->email, emailAddr2->email, 7) != 0);
}

PG_FUNCTION_INFO_V1(email_abs_cmp);

Datum
email_abs_cmp(PG_FUNCTION_ARGS) {
    EmailAddr *emailAddr1 = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr *emailAddr2 = (EmailAddr *) PG_GETARG_POINTER(1);
    PG_RETURN_INT32(intermediate(emailAddr1->email, emailAddr2->email, 0));
}


PG_FUNCTION_INFO_V1(email_hash_func);

Datum
email_hash_func(PG_FUNCTION_ARGS) {
    EmailAddr *emailAddr1 = (EmailAddr *) PG_GETARG_POINTER(0);
    PG_RETURN_INT32(hashFn(emailAddr1->email));
}

