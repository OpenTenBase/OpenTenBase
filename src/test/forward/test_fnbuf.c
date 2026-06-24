#include "postgres.h"
#include <unistd.h>
#include <stdlib.h> 
#include <time.h>
#include <CUnit/CUnit.h>
#include <CUnit/Automated.h>
#include <CUnit/TestDB.h>
#include <CUnit/Basic.h>
#include "storage/spin.h"
#include "forward/forward.h"
#include "forward/fnbufmgr.h"
#include "forward/fnpage.h"
#include "forward/fnmemory.h"
#include "forward/fnqueuemgr.h"
#include "forward/fnutils.h"
#include "forward/fnnodemgr.h"
//#include "forward/fnthreadmgr.h"
#include "forward/fnconf.h"
#include "pgxc/squeue.h"

#define FN_COMMON_MEM_SIZE  16384   /* 4*4096 */
const int fn_common_page_count = (FN_COMMON_MEM_SIZE / FnPageSize);
extern int fn_send_group_count_per_seg;
extern int64 fn_send_buf_pool_size;
extern int fn_send_buf_id_bound;

static bool
test_check_buf_list_with_type(FnBufferType buf_type, FnBufferList *buf_list, int exp_cnt)
{
    int act_count= 0;
    FnBufferId buf_id;

    if (buf_list->count != exp_cnt)
    {
        return false;
    }

    if (0 == exp_cnt)
    {
        if ((buf_list->first != FN_INVALID_BUFFER_ID) || (buf_list->first != FN_INVALID_BUFFER_ID))
        {
            return false;
        }
    }
    else if (1 == exp_cnt)
    {
        if (buf_list->first != buf_list->last)
        {
            return false;
        }
    }
    else
    {
        if (buf_list->first == buf_list->last)
        {
            return false;
        }

        buf_id = buf_list->first;
        while (buf_id != FN_INVALID_BUFFER_ID)
        {
            if (FN_BUFFER_TYPE_SND == buf_type)
            {
                if (buf_id >= fn_send_buf_id_bound)
                {
                    return false;
                }
            }
            else
            {
                if (buf_id < fn_send_buf_id_bound)
                {
                    return false;
                }
            }

            act_count++;
            buf_id = fn_get_next_buffer(buf_id);
        }

        if (act_count != exp_cnt)
        {
            return false;
        }
    }

    return true;
}


static void
test_one_buffer(FnBufferType buffer_type)
{
    FnBufferList buf_list;
    int alloc_count = 1;
    bool cmp_result;
    
    FnBufferInit(&buf_list);
    fn_alloc_buffer(buffer_type, 0, alloc_count, &buf_list);
    cmp_result = test_check_buf_list_with_type(buffer_type, &buf_list, alloc_count);
    CU_ASSERT_TRUE(cmp_result);

    CU_ASSERT_FALSE(FnBufferIsEmpty(&buf_list));
    CU_ASSERT_EQUAL(buf_list.first, buf_list.last);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);

    fn_free_buffer_list(&buf_list);

    cmp_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(cmp_result);
}

static void
test_snd_one_buffer(void)
{
    test_one_buffer(FN_BUFFER_TYPE_SND);
}

static void
test_recv_one_buffer(void)
{
    test_one_buffer(FN_BUFFER_TYPE_RECV);
}


static void
test_common_size(FnBufferType buf_type)
{
    FnBufferList buf_list;
    int alloc_count = fn_common_page_count;
    bool cmp_result;

    fn_alloc_buffer(buf_type, 0, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);

    cmp_result = test_check_buf_list_with_type(buf_type, &buf_list, alloc_count);
    CU_ASSERT_TRUE(cmp_result);

    fn_free_buffer_list(&buf_list);
}

static void
test_snd_common_size(void)
{
    test_common_size(FN_BUFFER_TYPE_SND);
}

static void
test_recv_common_size(void)
{
    test_common_size(FN_BUFFER_TYPE_RECV);
}


static void
test_less_common_size(FnBufferType buf_type)
{
    FnBufferList    buf_list;
    int alloc_count = fn_common_page_count - 1;
    bool cmp_result;

    fn_alloc_buffer(buf_type, 0, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    cmp_result = test_check_buf_list_with_type(buf_type, &buf_list, alloc_count);
    CU_ASSERT_TRUE(cmp_result);    

    fn_free_buffer_list(&buf_list);    
}

static void
test_snd_less_common_size(void)
{
    test_less_common_size(FN_BUFFER_TYPE_SND);
}

static void
test_recv_less_common_size(void)
{
    test_less_common_size(FN_BUFFER_TYPE_RECV);
}


static void
test_bigger_common_size(FnBufferType buf_type)
{
    FnBufferList buf_list;
    int alloc_count = fn_common_page_count + 1;
    bool cmp_result;

    fn_alloc_buffer(buf_type, 0, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    cmp_result = test_check_buf_list_with_type(buf_type, &buf_list, alloc_count);
    CU_ASSERT_TRUE(cmp_result);    

    fn_free_buffer_list(&buf_list);
}

static void
test_snd_bigger_common_size(void)
{
    test_bigger_common_size(FN_BUFFER_TYPE_SND);
}

static void
test_recv_bigger_common_size(void)
{
    test_bigger_common_size(FN_BUFFER_TYPE_RECV);
}


static void
test_double_common_size(FnBufferType buf_type)
{
    FnBufferList buf_list;
    int alloc_count = fn_common_page_count * 2;
    bool cmp_result;

    fn_alloc_buffer(buf_type, 0, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    cmp_result = test_check_buf_list_with_type(buf_type, &buf_list, alloc_count);
    CU_ASSERT_TRUE(cmp_result);

    fn_free_buffer_list(&buf_list);
}

static void
test_snd_double_common_size(void)
{
    test_double_common_size(FN_BUFFER_TYPE_SND);
}

static void
test_recv_double_common_size(void)
{
    test_double_common_size(FN_BUFFER_TYPE_RECV);
}


static void
test_bigger_double_common_size(FnBufferType buf_type)
{
    FnBufferList buf_list;
    int alloc_count = fn_common_page_count * 2 + 2;
    bool cmp_result;

    fn_alloc_buffer(buf_type, 0, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    cmp_result = test_check_buf_list_with_type(buf_type, &buf_list, alloc_count);
    CU_ASSERT_TRUE(cmp_result);    

    fn_free_buffer_list(&buf_list);
}

static void
test_snd_bigger_double_common_size(void)
{
    test_bigger_double_common_size(FN_BUFFER_TYPE_SND);
}

static void
test_recv_bigger_double_common_size(void)
{
    test_bigger_double_common_size(FN_BUFFER_TYPE_RECV);
}

static void
test_different_fid(FnBufferType buf_type)
{
    FnBufferList buf_list_0;
    int alloc_count0 = fn_common_page_count - 1;
    int fid0 = 0;

    FnBufferList buf_list_1;
    int alloc_count1 = fn_common_page_count - 2;
    int fid1 = 1;

    bool cmp_result;

    fn_alloc_buffer(buf_type, fid0, alloc_count0, &buf_list_0);
    CU_ASSERT_EQUAL(buf_list_0.count, alloc_count0);
    cmp_result = test_check_buf_list_with_type(buf_type, &buf_list_0, alloc_count0);
    CU_ASSERT_TRUE(cmp_result);

    fn_alloc_buffer(buf_type, fid1, alloc_count1, &buf_list_1);
    CU_ASSERT_EQUAL(buf_list_1.count, alloc_count1);
    cmp_result = test_check_buf_list_with_type(buf_type, &buf_list_1, alloc_count1);
    CU_ASSERT_TRUE(cmp_result);
    

    fn_free_buffer_list(&buf_list_0);
    fn_free_buffer_list(&buf_list_1);
}

static void
test_snd_different_fid(void)
{
    test_different_fid(FN_BUFFER_TYPE_SND);
}

static void
test_recv_different_fid(void)
{
    test_different_fid(FN_BUFFER_TYPE_RECV);
}


static void
test_cross_different_fid(FnBufferType buf_type)
{
    FnBufferList buf_list_0;
    int alloc_count0 = fn_common_page_count - 1;
    int fid0 = 0;

    FnBufferList buf_list_1;
    int alloc_count1 = fn_common_page_count - 2;
    int fid1 = 1;

    FnBufferList buf_list_2;
    int alloc_count2 = fn_common_page_count * 3;
    int fid2 = 0;

    fn_alloc_buffer(buf_type, fid0, alloc_count0, &buf_list_0);
    CU_ASSERT_EQUAL(buf_list_0.count, alloc_count0);

    fn_alloc_buffer(buf_type, fid1, alloc_count1, &buf_list_1);
    CU_ASSERT_EQUAL(buf_list_1.count, alloc_count1);

    fn_alloc_buffer(buf_type, fid2, alloc_count2, &buf_list_2);
    CU_ASSERT_EQUAL(buf_list_2.count, alloc_count2);
    

    fn_free_buffer_list(&buf_list_0);
    fn_free_buffer_list(&buf_list_1);
    fn_free_buffer_list(&buf_list_2);
}

static void
test_snd_cross_different_fid(void)
{
    test_cross_different_fid(FN_BUFFER_TYPE_SND);
}

static void
test_recv_cross_different_fid(void)
{
    test_cross_different_fid(FN_BUFFER_TYPE_RECV);
}


static void
test_alloc_several_times(FnBufferType buf_type)
{
    const int times = 5;
    FnBufferList *buf_list = malloc(times * sizeof(FnBufferList));
    int i = 0;

    int fid = 0;
    int alloc_count;
    
    for (i = 0; i < times; i++)
    {
        alloc_count = i + 1;
        fn_alloc_buffer(buf_type, fid, alloc_count, &buf_list[i]);
        CU_ASSERT_EQUAL(buf_list[i].count, alloc_count);
    }

    for (i = 0; i < times; i++)
    {
        fn_free_buffer_list(&buf_list[i]);
    }

    free(buf_list);
}

static void
test_snd_alloc_several_times(void)
{
    test_alloc_several_times(FN_BUFFER_TYPE_SND);
}

static void
test_recv_alloc_several_times(void)
{
    test_alloc_several_times(FN_BUFFER_TYPE_RECV);
}


static void
test_seg_size_from_start(FnBufferType buf_type)
{
    FnBufferList buf_list;
    int alloc_count = fn_common_page_count * fn_send_group_count_per_seg;
    int start_fid = 0;

    fn_alloc_buffer(buf_type, start_fid, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);

    fn_free_buffer_list(&buf_list);
}

static void
test_snd_seg_size_from_start(void)
{
    test_seg_size_from_start(FN_BUFFER_TYPE_SND);
}

static void
test_recv_seg_size_from_start(void)
{
    test_seg_size_from_start(FN_BUFFER_TYPE_RECV);
}


static void
test_seg_size_from_middle(FnBufferType buf_type)
{
    FnBufferList buf_list;
    int alloc_count = fn_common_page_count * fn_send_group_count_per_seg;
    int middle_fid = fn_send_group_count_per_seg + 1;

    fn_alloc_buffer(buf_type, middle_fid, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);

    fn_free_buffer_list(&buf_list);

    /* after all the allocate and deallocate, add one more check. */
    alloc_count = 3;
    FnBufferInit(&buf_list);
    fn_alloc_buffer(buf_type, middle_fid, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);

    fn_free_buffer_list(&buf_list);    
}

static void
test_snd_seg_size_from_middle(void)
{
    test_seg_size_from_middle(FN_BUFFER_TYPE_SND);
}

static void
test_recv_seg_size_from_middle(void)
{
    test_seg_size_from_middle(FN_BUFFER_TYPE_RECV);

}


static void
test_bigger_seg_size(FnBufferType buf_type)
{
    FnBufferList buf_list;
    int alloc_count = fn_common_page_count * fn_send_group_count_per_seg + 1;
    int start_fid = 0;

    fn_alloc_buffer(buf_type, start_fid, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);

    fn_free_buffer_list(&buf_list);

    /* after all the allocate and deallocate, add one more check. */
    alloc_count = 3;
    FnBufferInit(&buf_list);
    fn_alloc_buffer(buf_type, start_fid, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    fn_free_buffer_list(&buf_list);      
}

static void
test_snd_bigger_seg_size(void)
{
    test_bigger_seg_size(FN_BUFFER_TYPE_SND);
}

static void
test_recv_bigger_seg_size(void)
{
    test_bigger_seg_size(FN_BUFFER_TYPE_RECV);
}


static void
test_all_size(FnBufferType buf_type)
{
    FnBufferList buf_list;
    int alloc_count = fn_send_buf_pool_size / FnPageSize;
    int start_fid = 0;    

    fn_alloc_buffer(buf_type, 0, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);

    fn_free_buffer_list(&buf_list);

    /* after all the allocate and deallocate, add one more check. */
    alloc_count = 3;
    FnBufferInit(&buf_list);
    fn_alloc_buffer(buf_type, start_fid, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    fn_free_buffer_list(&buf_list);      
}

static void
test_snd_all_size(void)
{
    test_all_size(FN_BUFFER_TYPE_SND);
}

static void
test_recv_all_size(void)
{
    test_all_size(FN_BUFFER_TYPE_RECV);
}


static void
test_bigger_all_size(FnBufferType buf_type)
{
    FnBufferList buf_list;
    int max_size = fn_send_buf_pool_size / FnPageSize;
    int alloc_count = max_size + 10;    

    fn_alloc_buffer(buf_type, 0, alloc_count, &buf_list);
    CU_ASSERT_EQUAL(buf_list.count, max_size);

    fn_free_buffer_list(&buf_list);
}

static void
test_snd_bigger_all_size(void)
{
    test_bigger_all_size(FN_BUFFER_TYPE_SND);
}

static void
test_recv_bigger_all_size(void)
{
    test_bigger_all_size(FN_BUFFER_TYPE_RECV);
}

bool    stop_flag = false;
bool    start_flag = false;

static bool 
test_is_thread_start(void)
{
    return start_flag;
}

static bool
test_is_thread_stop(void)
{
    return stop_flag;
}

static void
test_start_thread(void)
{
    start_flag = true;
    stop_flag = false;
}

static void
test_stop_thread(void)
{
    stop_flag = true;
    start_flag = false;
}

typedef struct TestFnThreadArg
{
    int             thr_id;
    FnBufferType    buf_type;
    int             max_page_cnt;
    int             max_fid;
    bool            alloc_finish;
    bool            free_finish;
    bool            write_stop;
    slock_t         lock;
    FnBufferList    buf_list;
} TestFnThreadArg;



/* including both allocate and free */
static void *
test_alloc_free_thread(void *argv)
{
    TestFnThreadArg *thr_arg = argv;
    int             i;
    FnBufferList    buf_list;

    do
    {
        for (i = 0; i < GTM_MAX_FRAGMENTS; i++)
        {
            fn_alloc_buffer(thr_arg->buf_type, i, thr_arg->max_page_cnt, &buf_list);
            fn_free_buffer_list(&buf_list);
        }
    } while(!stop_flag);

	return NULL;
}


static void
test_alloc_free_by_two_threads(FnBufferType buf_type)
{
    bool                check_result = false;
    const int           duration = 1;
    int                 tick = 0;
    TestFnThreadArg     *thr_arg = NULL;
    thr_arg = malloc(sizeof(TestFnThreadArg));

    thr_arg->buf_type = buf_type;
    thr_arg->max_fid = GTM_MAX_FRAGMENTS;
    thr_arg->max_page_cnt = 1;

    /* create 1 thread */
    CreateThread(test_alloc_free_thread, thr_arg, MT_THR_DETACHED);

    /* create 2 thread */
    CreateThread(test_alloc_free_thread, thr_arg, MT_THR_DETACHED);

    while (tick < duration)
    {
        sleep(1);
        tick++;
    }

    stop_flag = true;
    sleep(2);
    check_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(check_result);
}

static void
test_snd_by_two_threads()
{
    test_alloc_free_by_two_threads(FN_BUFFER_TYPE_SND);
}


/* */
static void *
test_alloc_thread(void *argv)
{
    TestFnThreadArg         *thrd_arg = NULL;
    int                     i = 0;
    int                     page_cnt = 0;
    bool                    free_finish = false;

    while (!test_is_thread_start())
    {
    }

    srand((unsigned)time(NULL));
    thrd_arg = (TestFnThreadArg *)argv;
    do
    {
        for (i = 0; i < thrd_arg->max_fid; i++)
        {
            do
            {
                SpinLockAcquire(&thrd_arg->lock);
                free_finish = thrd_arg->free_finish;
                SpinLockRelease(&thrd_arg->lock);
            } while(!free_finish);

            FnBufferInit(&thrd_arg->buf_list);
            page_cnt = rand() % thrd_arg->max_page_cnt;
            fn_alloc_buffer(thrd_arg->buf_type, i, page_cnt, &thrd_arg->buf_list);
            SpinLockAcquire(&thrd_arg->lock);
            thrd_arg->alloc_finish = true;
            SpinLockRelease(&thrd_arg->lock);
        }
    } while(test_is_thread_stop());

	return NULL;

}

/* */
static void *
test_free_thread(void *argv)
{
    TestFnThreadArg         *thrd_arg = NULL;
    bool                    alloc_finish = false;

    while (!test_is_thread_start())
    {
    }

    thrd_arg = (TestFnThreadArg *)argv;
    do
    {
        do
        {
            SpinLockAcquire(&thrd_arg->lock);
            alloc_finish = thrd_arg->alloc_finish;
            SpinLockRelease(&thrd_arg->lock);
        } while(!alloc_finish);
        
        fn_free_buffer_list(&thrd_arg->buf_list);
        SpinLockAcquire(&thrd_arg->lock);
        thrd_arg->free_finish = true;
        thrd_arg->alloc_finish = false;
        SpinLockRelease(&thrd_arg->lock);
    } while(test_is_thread_stop());

    sleep(2);
    if (thrd_arg->alloc_finish)
    {
        fn_free_buffer_list(&thrd_arg->buf_list);    
    }

	return NULL;

}


static void
test_alloc_free_by_several_threads(FnBufferType buf_type)
{
    TestFnThreadArg     *thr_arg_arr = NULL;
    const int           thr_cnt = 1;
    int                 thr_id = 0;
    int                 max_page_cnt = 10;
    int                 duration = 1;
    int                 counter = 0;

    thr_arg_arr = malloc(thr_cnt * sizeof(TestFnThreadArg));

    for (thr_id = 0; thr_id < thr_cnt; thr_id++)
    {
        thr_arg_arr[thr_id].thr_id = thr_id;
        thr_arg_arr[thr_id].buf_type = buf_type;
        thr_arg_arr[thr_id].max_page_cnt = max_page_cnt;
        thr_arg_arr[thr_id].max_fid = GTM_MAX_FRAGMENTS;
        thr_arg_arr[thr_id].alloc_finish = false;
        thr_arg_arr[thr_id].free_finish = true;
        thr_arg_arr[thr_id].write_stop = false;
        SpinLockInit(&thr_arg_arr[thr_id].lock);
        FnBufferInit(&thr_arg_arr[thr_id].buf_list);
    }

    for (thr_id = 0; thr_id < thr_cnt; thr_id++)
    {
        CreateThread(test_alloc_thread, &thr_arg_arr[thr_id], MT_THR_DETACHED);
        CreateThread(test_free_thread, &thr_arg_arr[thr_id], MT_THR_DETACHED);
    }

    test_start_thread();
    do
    {
        sleep(10);
        counter++;
    } while(counter < duration);

    test_stop_thread();

    printf("wait for 5 seconds for all thread to finish.\n");
    sleep(5);
}

static void
test_snd_by_several_threads()
{
    bool check_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(check_result);  

    test_alloc_free_by_several_threads(FN_BUFFER_TYPE_SND);

    check_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(check_result);    
}


static void
test_fn_init_memory_mgr(void)
{
    fn_init_alldn_shm_mgr();
    fn_init_perdn_shm_mgr(1, true);
    fn_init_priv_mem_mgr();
}


static int
suite_snd_fnmgr_init()
{
    Size alldn_shm_size;
    void *alldn_addr;
    Size perdn_shm_size;
    void *perdn_addr;
    fn_init_queue_mgr();
    test_fn_init_memory_mgr();
    alldn_shm_size = fn_get_alldn_shm_size();
    alldn_addr = malloc(alldn_shm_size);

    fn_perdn_max_proc_count = 1024;
    fn_frag_window_size = 1024;

    fn_init_perdn_queue_attr(0);
    fn_register_perdn_shm(0, 1024, 1024);

    perdn_shm_size = fn_get_perdn_shm_size(0);
    perdn_addr = malloc(perdn_shm_size);
    fn_init_alldn_shm(alldn_addr, alldn_shm_size);
    fn_init_perdn_shm(0, perdn_addr, perdn_shm_size);
    fn_init_alldn_queue_mem();
    fn_create_queue(0);
    fn_init_queue_mem();
    return 0;
}

static int
suite_snd_fnmgr_clean()
{
    return 0;
}

static void
suite_snd_fnmgr_setup()
{
}

static void
suite_snd_fnmgr_teardown()
{
}

static int
suite_recv_fnmgr_init(void)
{
    return 0;
}

static int
suite_recv_fnmgr_clean(void)
{
    return 0;
}

static void
suite_recv_fnmgr_setup(void)
{
}


static void
suite_recv_fnmgr_teardown(void)
{
}


CU_TestInfo fnmgr_snd_testcases[] = 
{
    {"Testing alloc and free one buffer", test_snd_one_buffer},
    {"Testing alloc and free common size", test_snd_common_size},
    {"Testing alloc and free less than common size", test_snd_less_common_size},
    {"Testing alloc and free bigger than common size", test_snd_bigger_common_size},
    {"Testing alloc and free double common size", test_snd_double_common_size},
    {"Testing alloc and free bigger than double common size", test_snd_bigger_double_common_size},
    {"Testing alloc and free different fid", test_snd_different_fid},
    {"Testing alloc and free cross different fid", test_snd_cross_different_fid},
    {"Testing alloc and free several times", test_snd_alloc_several_times},
    {"Testing alloc and free whole segment size from 0 fid", test_snd_seg_size_from_start},
    {"Testing alloc and free whole segment size from middle fid", test_snd_seg_size_from_middle},
    {"Testing alloc and free bigger than one segment size", test_snd_bigger_seg_size},
    {"Testing alloc and free all buffer", test_snd_all_size},
    {"Testing alloc and free bigger than all buffer", test_snd_bigger_all_size},
    {"Testing alloc and free sending buffer by two threads", test_snd_by_two_threads},
    /*{"Testing two threads alloc, two threads free sending buffer", test_snd_by_several_threads},*/
    CU_TEST_INFO_NULL
};

CU_TestInfo fnmgr_recv_testcases[] = 
{
    {"Testing alloc and free one buffer", test_recv_one_buffer},
    {"Testing alloc and free common size", test_recv_common_size},
    {"Testing alloc and free less than common size", test_recv_less_common_size},
    {"Testing alloc and free bigger than common size", test_recv_bigger_common_size},
    {"Testing alloc and free double common size", test_recv_double_common_size},
    {"Testing alloc and free bigger than double common size", test_recv_bigger_double_common_size},
    {"Testing alloc and free different fid", test_recv_different_fid},
    {"Testing alloc and free cross different fid", test_recv_cross_different_fid},
    {"Testing alloc and free several times", test_recv_alloc_several_times}, 
    {"Testing alloc and free whole segment size from 0 fid", test_recv_seg_size_from_start},
    {"Testing alloc and free whole segment size from middle fid", test_recv_seg_size_from_middle},   
    {"Testing alloc and free bigger than one segment size", test_recv_bigger_seg_size},   
    {"Testing alloc and free all buffer", test_recv_all_size},  
    {"Testing alloc and free bigger than all buffer", test_recv_bigger_all_size},
    /*{"Testing alloc and free by two threads", test_recv_by_two_threads},    */
    CU_TEST_INFO_NULL
};


static int
suite_snd_fnque_init(void)
{
    return 0;
}

static int
suite_snd_fnque_clean(void)
{
    return 0;
}

static void
suite_snd_fnque_setup(void)
{
}

static void
suite_snd_fnque_teardown(void)
{
}


typedef struct TestBufferCache
{
    FnBufferId  *buf_arr;
    int         buf_count;
} TestBufferCache;

#define FnCacheInit(buf_cache) \
do\
{\
    (buf_cache)->buf_count = 0;\
    (buf_cache)->buf_arr = NULL;\
} while(0);\


static void
test_expand_buf_cache(TestBufferCache *buf_cache, int expand_cnt)
{
    int old_cnt = 0;
    int new_cnt = 0;
    void *new_arr = NULL;

    old_cnt = buf_cache->buf_count;
    new_cnt = buf_cache->buf_count + expand_cnt;
    new_arr = malloc(new_cnt * sizeof(FnBufferId));
    memset(new_arr, 0, new_cnt * sizeof(FnBufferId));
    memcpy(new_arr, buf_cache->buf_arr, old_cnt * sizeof(FnBufferId));
    buf_cache->buf_count = new_cnt;
    free(buf_cache->buf_arr);
    buf_cache->buf_arr = new_arr;
}

static void
test_cache_buf_list(FnBufferList *buf_list, TestBufferCache *buf_cache)
{
    FnBufferId buf_id;
    int i = 0;
    int old_cnt = 0;

    if (buf_cache->buf_count != 0)
    {
        i = buf_cache->buf_count;
        old_cnt = buf_cache->buf_count;
        test_expand_buf_cache(buf_cache, buf_list->count);
    }
    else
    {
        buf_cache->buf_count = buf_list->count;
        if (buf_cache->buf_count != 0)
        {
            buf_cache->buf_arr = malloc(sizeof(FnBufferId) * buf_list->count);    
        }
    }
    

    buf_id = buf_list->first;
    while (buf_id != FN_INVALID_BUFFER_ID)
    {
        if ((i - old_cnt) >= buf_list->count)
        {
            CU_ASSERT_FALSE(true);
            break;
        }

        buf_cache->buf_arr[i] = buf_id;
        buf_id = fn_get_next_buffer(buf_id);
        i++;
    }
}

static void
test_release_buf_cache(TestBufferCache *buf_cache)
{
    free(buf_cache->buf_arr);
    buf_cache->buf_arr = NULL;
    buf_cache->buf_count = 0;
}

static bool
test_cmp_buf_cache(FnBufferList *buf_list, TestBufferCache *buf_cache)
{
    FnBufferId buf_id;
    int i = 0;

    if (buf_list->count != buf_cache->buf_count)
    {
        return false;
    }

    buf_id = buf_list->first;
    while (buf_id != FN_INVALID_BUFFER_ID)
    {
        if (i >= buf_cache->buf_count)
        {
            return false;
        }

        if (buf_cache->buf_arr[i] != buf_id)
        {
            return false;
        }
        buf_id = fn_get_next_buffer(buf_id);
        i++;
    }

    if (i != buf_cache->buf_count)
    {
        return false;
    }

    return true;
}

static bool
test_cmp_one_buf_cache(int index, FnBufferId buf_id, TestBufferCache *buf_cache)
{
    if (index >= buf_cache->buf_count)
    {
        return false;
    }

    if (buf_id != buf_cache->buf_arr[index])
    {
        return false;
    }
    else
    {
        return true;
    }
}

static bool
test_cmp_part_buf_cache(FnBufferList *buf_list, int start_idx, int cmp_cnt, TestBufferCache *buf_cache)
{
    FnBufferId buf_id;
    int i = start_idx;

    if (buf_list->count > buf_cache->buf_count)
    {
        return false;
    }

    buf_id = buf_list->first;
    while (buf_id != FN_INVALID_BUFFER_ID)
    {
        if (i >= buf_cache->buf_count)
        {
            return false;
        }

        if (buf_cache->buf_arr[i] != buf_id)
        {
            return false;
        }
        buf_id = fn_get_next_buffer(buf_id);
        i++;
    }

    if (i != cmp_cnt + start_idx)
    {
        return false;
    }

    return true;
}


static bool
test_check_buf_list(FnBufferList *buf_list, int exp_cnt)
{
    int act_count= 0;
    FnBufferId buf_id;

    if (buf_list->count != exp_cnt)
    {
        return false;
    }

    if (0 == exp_cnt)
    {
        if ((buf_list->first != FN_INVALID_BUFFER_ID) || (buf_list->first != FN_INVALID_BUFFER_ID))
        {
            return false;
        }
    }
    else if (1 == exp_cnt)
    {
        if (buf_list->first != buf_list->last)
        {
            return false;
        }
    }
    else
    {
        if (buf_list->first == buf_list->last)
        {
            return false;
        }

        buf_id = buf_list->first;
        while (buf_id != FN_INVALID_BUFFER_ID)
        {
            act_count++;
            buf_id = fn_get_next_buffer(buf_id);
        }

        if (act_count != exp_cnt)
        {
            return false;
        }
    }

    return true;
}

static void
test_prod_one_cons_one(void)
{
    TestBufferCache buf_cache;
    FnBufferList buf_list;
    FnBufferList exp_buf_list;
    bool cmp_result;
    int alloc_count = 1;    

    FnBufferInit(&buf_list);
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    FnCacheInit(&buf_cache);
    test_cache_buf_list(&buf_list, &buf_cache);

    CU_ASSERT_FALSE(FnBufferIsEmpty(&buf_list));
    CU_ASSERT_EQUAL(buf_list.first, buf_list.last);
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);

    fn_push_queue(FN_QUEUE_SND, 0, 0, &buf_list);
    fn_pop_queue(FN_QUEUE_SND, 0, 0, alloc_count, &exp_buf_list);
    cmp_result = test_check_buf_list(&exp_buf_list, alloc_count);
    CU_ASSERT_TRUE(cmp_result);
    cmp_result = test_cmp_buf_cache(&exp_buf_list, &buf_cache);
    CU_ASSERT_TRUE(cmp_result);

    fn_free_buffer_list(&exp_buf_list);
    test_release_buf_cache(&buf_cache);
}

static void
test_prod_queue(FnQueueType que_type, int dn_id, int que_id, FnBufferList *buf_list, int count)
{
    FnBufferList    push_list;
    FnBufferId      buf_id;
    FnBufferInit(&push_list);

    push_list.first = buf_list->current;
    buf_id = buf_list->current;
    if (FN_INVALID_BUFFER_ID == buf_id)
    {
        return;
    }

    push_list.count++;
    while (buf_id != FN_INVALID_BUFFER_ID)
    {
        if (push_list.count == count)
        {
            push_list.last = buf_id;
            buf_list->current = fn_get_next_buffer(buf_id);
            fn_push_queue(que_type, 0, que_id, &push_list);
            return;
        }

        buf_id = fn_get_next_buffer(buf_id);
        buf_list->current = buf_id;
        push_list.count++;        
    }

    push_list.last = buf_id;
    fn_push_queue(que_type, 0, 0, &push_list);
}

static void
test_prod_one_by_one_cons_all(void)
{
    TestBufferCache buf_cache;
    FnBufferList buf_list;
    FnBufferList exp_buf_list;
    bool cmp_result;
    int alloc_count = 3;
    int i;

    FnBufferInit(&buf_list);
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    FnCacheInit(&buf_cache);
    test_cache_buf_list(&buf_list, &buf_cache);

    CU_ASSERT_FALSE(FnBufferIsEmpty(&buf_list));
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);

    for (i = 0; i < alloc_count; i++)
    {
        test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, 1);
    }

    fn_pop_queue(FN_QUEUE_SND, 0, 0, alloc_count, &exp_buf_list);

    cmp_result = test_check_buf_list(&exp_buf_list, alloc_count);
    CU_ASSERT_TRUE(cmp_result);
    cmp_result = test_cmp_buf_cache(&exp_buf_list, &buf_cache);
    CU_ASSERT_TRUE(cmp_result);

    fn_free_buffer_list(&exp_buf_list);
    test_release_buf_cache(&buf_cache);
}

static void
test_prod_list_cons_all(void)
{
    TestBufferCache buf_cache;
    FnBufferList buf_list;
    FnBufferList exp_buf_list;
    bool cmp_result;
    int alloc_count = fn_common_page_count + 1;

    FnBufferInit(&buf_list);
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    FnCacheInit(&buf_cache);
    test_cache_buf_list(&buf_list, &buf_cache);

    CU_ASSERT_FALSE(FnBufferIsEmpty(&buf_list));
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);

    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);
    fn_pop_queue(FN_QUEUE_SND, 0, 0, alloc_count, &exp_buf_list);
    cmp_result = test_check_buf_list(&exp_buf_list, alloc_count);
    CU_ASSERT_TRUE(cmp_result);    
    cmp_result = test_cmp_buf_cache(&exp_buf_list, &buf_cache);
    CU_ASSERT_TRUE(cmp_result);

    fn_free_buffer_list(&exp_buf_list);
    test_release_buf_cache(&buf_cache);
}

static void
test_prod_two_list_cons_all(void)
{
    TestBufferCache buf_cache;
    FnBufferList buf_list;
    FnBufferList exp_buf_list;
    bool cmp_result;
    int alloc_count = fn_common_page_count + 1;
    int pop_count = 0;

    FnCacheInit(&buf_cache);
    FnBufferInit(&buf_list);
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    CU_ASSERT_FALSE(FnBufferIsEmpty(&buf_list));
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    test_cache_buf_list(&buf_list, &buf_cache);

    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);
    pop_count = pop_count + alloc_count;

    FnBufferInit(&buf_list);
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    CU_ASSERT_FALSE(FnBufferIsEmpty(&buf_list));
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    test_cache_buf_list(&buf_list, &buf_cache);

    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);
    pop_count = pop_count + alloc_count;


    fn_pop_queue(FN_QUEUE_SND, 0, 0, pop_count, &exp_buf_list);
    cmp_result = test_check_buf_list(&exp_buf_list, pop_count);
    CU_ASSERT_TRUE(cmp_result);    
    cmp_result = test_cmp_buf_cache(&exp_buf_list, &buf_cache);
    CU_ASSERT_TRUE(cmp_result);

    fn_free_buffer_list(&exp_buf_list);
    test_release_buf_cache(&buf_cache);

}

static void
test_prod_two_list_cons_one(void)
{
    TestBufferCache buf_cache;
    FnBufferList buf_list;
    FnBufferList exp_buf_list;
    bool cmp_result;
    int alloc_count = fn_common_page_count - 1;
    int pop_count = 0;
    int i;

    FnCacheInit(&buf_cache);
    FnBufferInit(&buf_list);
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    CU_ASSERT_FALSE(FnBufferIsEmpty(&buf_list));
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    test_cache_buf_list(&buf_list, &buf_cache);

    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);
    pop_count = pop_count + alloc_count;

    FnBufferInit(&buf_list);
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    CU_ASSERT_FALSE(FnBufferIsEmpty(&buf_list));
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    test_cache_buf_list(&buf_list, &buf_cache);

    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);
    pop_count = pop_count + alloc_count;

    for (i = 0; i < pop_count; i++)
    {
        FnBufferInit(&exp_buf_list);
        fn_pop_queue(FN_QUEUE_SND, 0, 0, 1, &exp_buf_list);
        /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
        CU_ASSERT_EQUAL(exp_buf_list.count, 1);
        cmp_result = test_check_buf_list(&exp_buf_list, 1);
        CU_ASSERT_TRUE(cmp_result);
        cmp_result = test_cmp_one_buf_cache(i, exp_buf_list.first, &buf_cache);
        CU_ASSERT_TRUE(cmp_result);
        fn_free_buffer_list(&exp_buf_list);        
    }

    test_release_buf_cache(&buf_cache);    
}


static void
test_prod_two_list_cons_same_order(void)
{
    TestBufferCache buf_cache;
    FnBufferList buf_list;
    FnBufferList exp_buf_list;
    bool cmp_result;
    int alloc_count = 0;
    int pop_count = 0;

    FnCacheInit(&buf_cache);
    FnBufferInit(&buf_list);

    alloc_count = fn_common_page_count + 1;
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    test_cache_buf_list(&buf_list, &buf_cache);

    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);
    pop_count = pop_count + alloc_count;

    FnBufferInit(&buf_list);
    alloc_count = fn_common_page_count + 2;
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    test_cache_buf_list(&buf_list, &buf_cache);

    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);

    /* consume the first queue. */
    FnBufferInit(&exp_buf_list);
    pop_count = fn_common_page_count + 1;
    fn_pop_queue(FN_QUEUE_SND, 0, 0, pop_count, &exp_buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(exp_buf_list.count, pop_count);
    cmp_result = test_check_buf_list(&exp_buf_list, pop_count);
    CU_ASSERT_TRUE(cmp_result);
    cmp_result = test_cmp_part_buf_cache(&exp_buf_list, 0, pop_count, &buf_cache);
    CU_ASSERT_TRUE(cmp_result);
    fn_free_buffer_list(&exp_buf_list);
        
    /* consume the second queue */
    FnBufferInit(&exp_buf_list);
    pop_count = fn_common_page_count + 2;
    fn_pop_queue(FN_QUEUE_SND, 0, 0, pop_count, &exp_buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(exp_buf_list.count, pop_count);
    cmp_result = test_check_buf_list(&exp_buf_list, pop_count);
    CU_ASSERT_TRUE(cmp_result);
    cmp_result = test_cmp_part_buf_cache(&exp_buf_list, fn_common_page_count + 1, pop_count, &buf_cache);
    CU_ASSERT_TRUE(cmp_result);
    fn_free_buffer_list(&exp_buf_list);    

    test_release_buf_cache(&buf_cache);
}

static void
test_prod_two_list_cons_diff_order(void)
{
    TestBufferCache buf_cache;
    FnBufferList buf_list;
    FnBufferList exp_buf_list;
    bool cmp_result;
    int alloc_count = 0;
    int pop_count = 0;

    FnCacheInit(&buf_cache);
    FnBufferInit(&buf_list);

    alloc_count = fn_common_page_count + 2;
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    test_cache_buf_list(&buf_list, &buf_cache);

    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);
    pop_count = pop_count + alloc_count;

    FnBufferInit(&buf_list);
    alloc_count = fn_common_page_count + 1;
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    test_cache_buf_list(&buf_list, &buf_cache);

    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);

    /* consume the first queue. */
    FnBufferInit(&exp_buf_list);
    pop_count = fn_common_page_count + 2;
    fn_pop_queue(FN_QUEUE_SND, 0, 0, pop_count, &exp_buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(exp_buf_list.count, pop_count);
    cmp_result = test_check_buf_list(&exp_buf_list, pop_count);
    CU_ASSERT_TRUE(cmp_result);
    cmp_result = test_cmp_part_buf_cache(&exp_buf_list, 0, pop_count, &buf_cache);
    CU_ASSERT_TRUE(cmp_result);
    fn_free_buffer_list(&exp_buf_list);
        
    /* consume the second queue */
    FnBufferInit(&exp_buf_list);
    pop_count = fn_common_page_count + 1;
    fn_pop_queue(FN_QUEUE_SND, 0, 0, pop_count, &exp_buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(exp_buf_list.count, pop_count);
    cmp_result = test_check_buf_list(&exp_buf_list, pop_count);
    CU_ASSERT_TRUE(cmp_result);
    cmp_result = test_cmp_part_buf_cache(&exp_buf_list, fn_common_page_count + 2, pop_count, &buf_cache);
    CU_ASSERT_TRUE(cmp_result);
    fn_free_buffer_list(&exp_buf_list);    

    test_release_buf_cache(&buf_cache);

}

static void
test_prod_two_list_cons_more_size(void)
{
    TestBufferCache buf_cache;
    FnBufferList buf_list;
    FnBufferList exp_buf_list;
    bool cmp_result;
    int alloc_count = 0;
    int push_count = 0;
    int pop_count = 0;

    FnCacheInit(&buf_cache);
    FnBufferInit(&buf_list);

    alloc_count = fn_common_page_count + 2;
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    test_cache_buf_list(&buf_list, &buf_cache);

    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);
    pop_count = pop_count + alloc_count;
    push_count = push_count + alloc_count;

    FnBufferInit(&buf_list);
    alloc_count = fn_common_page_count + 1;
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    test_cache_buf_list(&buf_list, &buf_cache);
    pop_count = pop_count + alloc_count;
    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);
    push_count = push_count + alloc_count;    

    /* consume the queue. */
    pop_count = pop_count + 100;
    FnBufferInit(&exp_buf_list);
    fn_pop_queue(FN_QUEUE_SND, 0, 0, pop_count, &exp_buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(exp_buf_list.count, push_count);
    cmp_result = test_check_buf_list(&exp_buf_list, push_count);
    CU_ASSERT_TRUE(cmp_result);
    cmp_result = test_cmp_buf_cache(&exp_buf_list, &buf_cache);
    CU_ASSERT_TRUE(cmp_result);
    fn_free_buffer_list(&exp_buf_list);

    test_release_buf_cache(&buf_cache);

}

static void
test_prod_two_list_cons_unlimit_size(void)
{
    TestBufferCache buf_cache;
    FnBufferList buf_list;
    FnBufferList exp_buf_list;
    bool cmp_result;
    int alloc_count = 0;
    int push_count = 0;
    int pop_count = -1;

    bool memory_result = false;

    memory_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(memory_result);

    FnCacheInit(&buf_cache);
    FnBufferInit(&buf_list);

    alloc_count = fn_common_page_count + 3;
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    test_cache_buf_list(&buf_list, &buf_cache);

    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);
    push_count = push_count + alloc_count;

    FnBufferInit(&buf_list);
    alloc_count = fn_common_page_count + 5;
    fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, alloc_count, &buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(buf_list.count, alloc_count);
    test_cache_buf_list(&buf_list, &buf_cache);
    test_prod_queue(FN_QUEUE_SND, 0, 0, &buf_list, alloc_count);
    push_count = push_count + alloc_count;    

    /* consume the queue. */
    FnBufferInit(&exp_buf_list);
    fn_pop_queue(FN_QUEUE_SND, 0, 0, pop_count, &exp_buf_list);
    /*CU_ASSERT_FALSE(FnBufferIsEmpty(&exp_buf_list));*/
    CU_ASSERT_EQUAL(exp_buf_list.count, push_count);
    cmp_result = test_check_buf_list(&exp_buf_list, push_count);
    CU_ASSERT_TRUE(cmp_result);
    cmp_result = test_cmp_buf_cache(&exp_buf_list, &buf_cache);
    CU_ASSERT_TRUE(cmp_result);
    fn_free_buffer_list(&exp_buf_list);

    test_release_buf_cache(&buf_cache);  

    memory_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(memory_result);    
}

static void
test_prod_several_cons_several(void)
{
    int                 buf_cnt = 1;
    int                 i = 0;
    FnBufferList        buf_list;
    bool                check_result = false;

    check_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(check_result);

    for (i = 1; i <= buf_cnt; i++)
    {
        FnBufferInit(&buf_list);
        fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, i, &buf_list);

        /* pruduce queue */
        fn_push_queue(FN_QUEUE_RECV_FRAG, 0, 0, &buf_list);
    }
    
    for (i = 1; i <= buf_cnt; i++)
    {
        FnBufferInit(&buf_list);            
        fn_pop_queue(FN_QUEUE_RECV_FRAG, 0, 0, i, &buf_list);

        fn_free_buffer_list(&buf_list);
    }

    check_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(check_result);
}

static void
test_prod_cons_loop(void)
{
    int                 buf_cnt = 10;
    int                 i = 0;
    FnBufferList        buf_list;
    bool                check_result = false;

    check_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(check_result);

    for (i = 1; i <= buf_cnt; i++)
    {
        /* allocate and pruduce queue */
        FnBufferInit(&buf_list);
        fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, i, &buf_list);
        fn_push_queue(FN_QUEUE_RECV_FRAG, 0, 0, &buf_list);

        /* consume queue and free*/
        FnBufferInit(&buf_list);            
        fn_pop_queue(FN_QUEUE_RECV_FRAG, 0, 0, i, &buf_list);

        fn_free_buffer_list(&buf_list);        
    }

    check_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(check_result);    
}

typedef struct TestFnQueueArg
{
    int         thr_id;
    int64       write_cnt;
    int64       read_cnt;
} TestFnQueueArg;

static void
test_init_queue_arg(TestFnQueueArg *args, int arg_cnt)
{
    int         i = 0;
    for (i = 0; i < arg_cnt; i++)
    {
        args[i].thr_id = 0;
        args[i].write_cnt = 0;
        args[i].read_cnt = 0;
    }
}

static bool
test_cmp_write_read(TestFnQueueArg *args, int arg_cnt)
{
    int64       write_cnt = 0;
    int64       read_cnt = 0;
    int         i = 0;
    for (i = 0; i < arg_cnt; i++)
    {
        write_cnt = args[i].write_cnt + write_cnt;
        read_cnt = args[i].read_cnt + read_cnt;
    }

    if (write_cnt != read_cnt)
    {
        return false;
    }
    else
    {
        return true;
    }
}

static void *
test_write_thread(void *argv)
{
    int                 buf_cnt = 10;
    int                 i = 0;
    FnBufferList        buf_list;
    TestFnQueueArg      *que_arg = (TestFnQueueArg *)argv;

    while (!test_is_thread_start())
    {
    }

    do
    {
        for (i = 1; i <= buf_cnt; i++)
        {
            FnBufferInit(&buf_list);
            fn_alloc_buffer(FN_BUFFER_TYPE_SND, 0, i, &buf_list);

            /* pruduce queue */
            fn_push_queue(FN_QUEUE_RECV_FRAG, 0, 0, &buf_list);
            que_arg->write_cnt = que_arg->write_cnt + buf_list.count;
        }
    }while(!test_is_thread_stop());

    return NULL;
}

static void *
test_read_thread(void *argv)
{
    int                 buf_cnt = 10;
    int                 i = 0;
    FnBufferList        buf_list;
    TestFnQueueArg      *que_arg = (TestFnQueueArg *)argv;

    while (!test_is_thread_start())
    {
    }

    do
    {
        for (i = 1; i <= buf_cnt; i++)
        {
            FnBufferInit(&buf_list);            
            fn_pop_queue(FN_QUEUE_RECV_FRAG, 0, 0, -1, &buf_list);
            que_arg->read_cnt = que_arg->read_cnt + buf_list.count;
            fn_free_buffer_list(&buf_list);
        }
    } while(!test_is_thread_stop());

    /* finally, fetch all at once */
    sleep(5);

    fn_pop_queue(FN_QUEUE_RECV_FRAG, 0, 0, -1, &buf_list);
    que_arg->read_cnt = que_arg->read_cnt + buf_list.count;
    fn_free_buffer_list(&buf_list);
    return NULL;
}


static void
test_one_write_one_read_no_lock(void)
{
    bool                check_result = false;
    int                 wait_cycle = 10;
    int                 counter = 0;
    TestFnQueueArg      *que_arg = NULL;
    const int           thrd_cnt = 2;

    check_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(check_result);

    que_arg = malloc(thrd_cnt * sizeof(TestFnQueueArg));

    que_arg[0].thr_id = 0;
    que_arg[0].write_cnt = 0;
    que_arg[0].read_cnt = 0;
    /* create one write thread */
    CreateThread(test_write_thread, &que_arg[0], MT_THR_DETACHED);

    que_arg[1].thr_id = 0;
    que_arg[1].write_cnt = 0;
    que_arg[1].read_cnt = 0;
    /* create one read thread */
    CreateThread(test_read_thread, &que_arg[1], MT_THR_DETACHED);

    test_start_thread();
    do
    {
        sleep(1);
        counter++;
    } while(counter < wait_cycle);

    test_stop_thread();
    sleep(10);

    CU_ASSERT_TRUE(test_cmp_write_read(que_arg, thrd_cnt));

    check_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(check_result);
}

static void
test_several_write_one_read_with_lock(void)
{
    bool                check_result = false;
    int                 wait_cycle = 10;
    int                 counter = 0;
    int                 thr_cnt = 5;
    int                 thr_id = 0;
    TestFnQueueArg      *que_arg = NULL;

    check_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(check_result);

    que_arg = malloc((thr_cnt + 1) * sizeof(TestFnQueueArg));
    
    
    for (thr_id = 0; thr_id < thr_cnt; thr_id++)
    {
        que_arg[thr_id].thr_id = thr_id;
        que_arg[thr_id].read_cnt = 0;
        que_arg[thr_id].write_cnt = 0;
        /* create one write thread */
        CreateThread(test_write_thread, &que_arg[thr_id], MT_THR_DETACHED);    
    }
    
    /* create one read thread */
    que_arg[thr_cnt].thr_id = thr_cnt;
    que_arg[thr_cnt].read_cnt = 0;
    que_arg[thr_cnt].write_cnt = 0;
    CreateThread(test_read_thread, &que_arg[thr_cnt], MT_THR_DETACHED);

    test_start_thread();
    do
    {
        sleep(1);
        counter++;
    } while(counter < wait_cycle);

    test_stop_thread();
    sleep(10);

    CU_ASSERT_TRUE(test_cmp_write_read(que_arg, thr_cnt + 1));

    check_result = fn_verify_buffer_pool(FN_BUFFER_STATUS_FREE);
    CU_ASSERT_TRUE(check_result);    

}


static void
test_several_write_several_read_with_lock(void)
{

}

static void
test_one_write_several_read_with_lock(void)
{
    
}




CU_TestInfo fnquemgr_testcases[] = 
{
    {"Testing produce one item,", test_prod_one_cons_one},
    {"Testing produce several item, one by one, consume all", test_prod_one_by_one_cons_all},
    {"Testing produce a list, consume all", test_prod_list_cons_all},
    {"Testing produce two list consume all", test_prod_two_list_cons_all},
    {"Testing produce two list consume one item by one", test_prod_two_list_cons_one},
    {"Testing produce two list consume one list same order", test_prod_two_list_cons_same_order},
    {"Testing produce two list consume one list different order", test_prod_two_list_cons_diff_order},
    {"Testing produce two list consume more than list size.", test_prod_two_list_cons_more_size},
    {"Testing produce two list consume unlimited size.", test_prod_two_list_cons_unlimit_size},
    {"Testing produce lists with different size and consumes with different size", test_prod_several_cons_several},
    {"Testing produce and consumue list with different size loop", test_prod_cons_loop},
    {"Testing two threads, one write, one read, nolock.", test_one_write_one_read_no_lock},
    {"Testing one thread write, several threads write, with lock", test_one_write_several_read_with_lock},
    {"Testing several threads write, one thread read with lock", test_several_write_one_read_with_lock},
    {"Testing several threads write, several thread write with lock", test_several_write_several_read_with_lock},
    /*{"Testing 1000 threads write"}*/
    CU_TEST_INFO_NULL
};

/*
static int
suite_fnthrd_init(void)
{
    return fn_create_thread();
}

static int
suite_fnthrd_clean(void)
{
    while (1)
    {
        sleep(1);
    }
    return 0;
}

static void
suite_fnthrd_setup(void)
{
}

static void
suite_fnthrd_teardown(void)
{
}


CU_TestInfo fnthrdmgr_testcases[] = 
{
    
    CU_TEST_INFO_NULL
};
*/

CU_SuiteInfo fnbuf_suites[] = {
    { "Testing the forward buffer management for sender", suite_snd_fnmgr_init, suite_snd_fnmgr_clean, suite_snd_fnmgr_setup, suite_snd_fnmgr_teardown, fnmgr_snd_testcases},
    { "Testing the forward buffer management for receiver", suite_recv_fnmgr_init, suite_recv_fnmgr_clean, suite_recv_fnmgr_setup, suite_recv_fnmgr_teardown, fnmgr_recv_testcases},
    { "Testing the forward queue management", suite_snd_fnque_init, suite_snd_fnque_clean, suite_snd_fnque_setup, suite_snd_fnque_teardown, fnquemgr_testcases},
    //{ "Testing the forward thread management", suite_fnthrd_init, suite_fnthrd_clean, suite_fnthrd_setup, suite_fnthrd_teardown, fnthrdmgr_testcases},
    CU_SUITE_INFO_NULL
};

int main(int argc, char **argv)
{
    CU_BasicRunMode mode = CU_BRM_VERBOSE;
    CU_ErrorAction error_action = CUEA_IGNORE;
    int ret;

    ret = CU_initialize_registry();
    if (ret != CUE_SUCCESS)
    {
        fprintf(stderr, "fail to initialize register, error message %s.\n", CU_get_error_msg());
        return 0;
    }

    ret = CU_register_suites(fnbuf_suites);
    if (ret != CUE_SUCCESS)
    {
        fprintf(stderr, "fail to register suite, error message %s.\n", CU_get_error_msg());
        return 0;    
    }

    CU_basic_set_mode(mode);
    CU_set_error_action(error_action);

    ret = CU_basic_run_tests();
    fprintf(stderr, "run test case with return code %d.\n", ret);

    CU_cleanup_registry();
    return 0;

}

