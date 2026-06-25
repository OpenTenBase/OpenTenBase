


#include "postgres.h"
#include "fmgr.h"
#include "access/csnlog.h"
#include "access/lru.h"
#include "access/xact.h"
#include "common/zstd_compress.h"
#include "postmaster/bgwriter.h"
#include "storage/procarray.h"
#include <time.h>

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(test_compress_and_decompress);


uint64
generateRandom64BitNumber() {
    uint64 randomNum = 0;
    for (int i = 0; i < 8; i++) {
        randomNum = (randomNum << 8) | (rand() & 0xFF);
    }
    return randomNum;
}

bool
generate_zero_numbers_file(const char* filename, long long size)
{
    FILE* fp = fopen(filename, "wb");
    long long expect_len = size;

    if (fp == NULL)
    {
        ereport(WARNING, (errmsg("file %s open failed", filename)));
        return false;
    }
    const int bufferSize = 1024;
    uint64 buffer[bufferSize];

    while (size > 0) {
        int bytesToWrite = (size < bufferSize * 8) ? size : bufferSize * 8;

        for (int i = 0; i < bufferSize; i++) {
            buffer[i] = 0;  // Generate zero byte
        }

        fwrite(buffer, 1, bytesToWrite, fp);
        size -= bytesToWrite;
    }

    fseek(fp, 0, SEEK_END);
    long long write_len = ftell(fp);
    
    fclose(fp);
    ereport(LOG, (errmsg("file %s expect len:%ld, write:%ld", filename, expect_len, write_len)));
    return (write_len == expect_len);
}

bool 
generate_random_numbers_file(const char* filename, long long size)
{
    FILE* fp = fopen(filename, "wb");
    long long expect_len = size;
    
    if (fp == NULL)
    {
        ereport(WARNING, (errmsg("file %s open failed", filename)));
        return false;
    }
    srand(time(NULL));

    const int bufferSize = 1024;
    uint64 buffer[bufferSize];

    while (size > 0) 
    {
        int bytesToWrite = (size < bufferSize * 8) ? size : bufferSize * 8;

        for (int i = 0; i < bufferSize; i++) {
            buffer[i] = generateRandom64BitNumber();  // Generate random byte
        }

        fwrite(buffer, 1, bytesToWrite, fp);
        size -= bytesToWrite;
    }

    fseek(fp, 0, SEEK_END);
    long long write_len = ftell(fp);
    
    fclose(fp);
    ereport(LOG, (errmsg("file %s expect len:%ld, write:%ld", filename, expect_len, write_len)));
    return (write_len == expect_len);
}


bool 
generate_increase_numbers_file(const char* filename, long long size)
{
    FILE* fp = fopen(filename, "wb");
    long long expect_len = size;
    if (fp == NULL)
    {
        ereport(WARNING, (errmsg("file %s open failed", filename)));
        return false;
    }
    srand(time(NULL));

    const int bufferSize = 1024;
    uint64 buffer[bufferSize];

    while (size > 0) {
        uint64 randomNum = generateRandom64BitNumber();
        int bytesToWrite = (size < bufferSize * 8) ? size : bufferSize*8;

        for (int i = 0; i < bufferSize; i++) {
                buffer[i] = randomNum + i * 24 * 3600 + rand() % 256;
        }

        fwrite(buffer, 1, bytesToWrite, fp);
        size -= bytesToWrite;
    }

    fseek(fp, 0, SEEK_END);
    long long write_len = ftell(fp);
    
    fclose(fp);
    ereport(LOG, (errmsg("file %s expect len:%ld, write:%ld", filename, expect_len, write_len)));
    return (write_len == expect_len);
}



bool 
test_compress_and_decompress_file(const char *file_path)
{
    const int FILE_NAME_SIZE = 1024;
    char compress_file_path[FILE_NAME_SIZE];
    snprintf(compress_file_path, FILE_NAME_SIZE, "%s.cps", file_path);
    unlink(compress_file_path);
    CompressResource *res = simple_init_compress_resource();

    if (compress_file(res, file_path, compress_file_path) != 0)
    {
        ereport(WARNING, (errmsg("file %s compress failed:%s", file_path, res->errormsg_buf)));
        free_compress_resource(res);
        unlink(compress_file_path);
        return false;
    }

    free_compress_resource(res);

    char decompress_file_path[FILE_NAME_SIZE];
    snprintf(decompress_file_path, FILE_NAME_SIZE, "%s.new", file_path);
    unlink(decompress_file_path);
    DecompressResource *de_res = simple_init_decompress_resource();

    if (decompress_file(de_res, compress_file_path, decompress_file_path) != 0)
    {
        ereport(WARNING, (errmsg("file %s decompress failed:%s", compress_file_path, de_res->errormsg_buf)));
        free_decompress_resource(de_res);
        unlink(compress_file_path);
        unlink(decompress_file_path);
        return false;
    }

    free_decompress_resource(de_res);

    bool ret = CompareFiles(file_path, decompress_file_path);
    ereport(LOG, (errmsg("file %s and file %s compare result:%d", compress_file_path, decompress_file_path, ret)));
    unlink(compress_file_path);
    unlink(decompress_file_path);

    return ret;
}

Datum
test_compress_and_decompress(PG_FUNCTION_ARGS)
{
    const char *file1 = "all_zero_file"; // all zero
    unlink(file1);
    const char *file2 = "random_number_file"; // all random
    unlink(file2);
    const char *file3 = "increase_file"; // random start and increasing systematically
    unlink(file3);

    long long file_size = BLCKSZ * CSNLOG_XACTS_PER_LSN_GROUP;
    
    if (!generate_zero_numbers_file(file1, file_size))
    {
        unlink(file1);
        ereport(WARNING, (errmsg("file %s create failed", file1)));
        return false;
    }

    if (test_compress_and_decompress_file(file1) != 0)
    {
        unlink(file1);
        ereport(WARNING, (errmsg("file %s compress and decompress failed", file1)));
        return false;
    }
    
    unlink(file1);

    if (!generate_random_numbers_file(file2, file_size))
    {
        unlink(file2);
        ereport(WARNING, (errmsg("file %s create failed", file2)));
        return false;
    }

    if (test_compress_and_decompress_file(file2) != 0)
    {
        unlink(file2);
        ereport(WARNING, (errmsg("file %s compress and decompress failed", file2)));
        return false;
    }

    unlink(file2);

    if (!generate_increase_numbers_file(file3, file_size))
    {
        unlink(file3);
        ereport(WARNING, (errmsg("file %s create failed", file3)));
        return false;
    }

    if (test_compress_and_decompress_file(file3) != 0)
    {
        unlink(file3);
        ereport(WARNING, (errmsg("file %s compress and decompress failed", file3)));
        return false;
    }
    
    unlink(file3);
    PG_RETURN_BOOL(true);
}

