	/*-------------------------------------------------------------------------
	 *
	 *      main.c - 这是PostgreSQL服务器的主要C源文件，包含了所有PostgreSQL进程
	 * （包括postmaster、独立后端、独立引导进程或postmaster的单独执行子进程）的入口点。
	 
	 * This does some essential startup tasks for any incarnation of postgres
	 * (postmaster, standalone backend, standalone bootstrap process, or a
	 * separately exec'd child of a postmaster) and then dispatches to the
	 * proper FooMain() routine for the incarnation.
	 *
	 *
	 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
	 * Portions Copyright (c) 1994, Regents of the University of California
	 *
	 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
	 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
	 *
	 * IDENTIFICATION
	 *      src/backend/main/main.c
	 *
	 *-------------------------------------------------------------------------
	 */
	#include "postgres.h" // 包含PostgreSQL的核心头文件
	
	// 根据平台包含特定的头文件
	#include <unistd.h> // 通用UNIX系统调用和符号常量
	
	// 特定于NetBSD系统的头文件
	#if defined(__NetBSD__)
	#include <sys/param.h>
	#endif
	
	// 特定于x64架构和MSVC 1800（Visual Studio 2013）的头文件
	#if defined(_M_AMD64) && _MSC_VER == 1800
	#include <math.h> // 数学函数
	#include <versionhelpers.h> // 版本帮助库
	#endif
	
	// 其他必要的头文件
	#include "bootstrap/bootstrap.h" // 引导相关
	#include "common/username.h" // 用户名处理
	#include "port/atomics.h" // 原子操作
	#include "postmaster/postmaster.h" // postmaster进程管理
	#include "storage/s_lock.h" // 存储层自旋锁
	#include "storage/spin.h" // 存储层自旋锁
	#include "tcop/tcopprot.h" // 事务命令协议
	#include "utils/help_config.h" // 帮助配置
	#include "utils/memutils.h" // 内存管理
	#include "utils/pg_locale.h" // 本地化支持
	#include "utils/ps_status.h" // 进程状态
	
	
	const char *progname;
	const char *exename;
	
	
	static void startup_hacks(const char *progname);
	// 初始化本地化环境
	static void init_locale(const char *categoryname, int category, const char *locale);
	// 显示帮助信息
	static void help(const char *progname);
	// 检查是否以root用户身份运行
	static void check_root(const char *progname);
	
	
	/*
	 *  main函数 - PostgreSQL服务器进程的入口点。
	 */
	int
	main(int argc, char *argv[])
	{
		bool		do_check_root = true;
	
		progname = get_progname(argv[0]);
		/*
		 * Make a copy. Leaks memory, but called only once.
	         * 复制可执行文件名称，内存泄漏但仅调用一次
		 */
		exename = strdup(argv[0]);
	
		/*
		 * Platform-specific startup hacks
	  	 * 执行平台特定的启动处理	
		 */
		startup_hacks(progname);
	
		/*
		 * Remember the physical location of the initially given argv[] array for
		 * possible use by ps display.  On some platforms, the argv[] storage must
		 * be overwritten in order to set the process title for ps. In such cases
		 * save_ps_display_args makes and returns a new copy of the argv[] array.
		 *
		 * save_ps_display_args may also move the environment strings to make
		 * extra room. Therefore this should be done as early as possible during
		 * startup, to avoid entanglements with code that might save a getenv()
		 * result pointer.
		 */
	
		/*
		 * 记住最初给出的argv[]数组的物理位置，以便ps显示时可能使用。在某些平台上，为了
	         * 设置ps的进程标题，必须覆盖argv[]存储。在这种情况下，save_ps_display_args会创
		 * 建并返回argv[]数组的一个新副本。
		 *
		 * save_ps_display_args也可能移动环境字符串以腾出额外空间。因此，这应该在启动过程
	         * 中尽早完成，以避免与可能保存getenv()结果指针的代码产生冲突。
		 */
		
		argv = save_ps_display_args(argc, argv);
	
		/*
		 * If supported on the current platform, set up a handler to be called if
		 * the backend/postmaster crashes with a fatal signal or exception.
		 */
	#if defined(WIN32) && defined(HAVE_MINIDUMP_TYPE)
	    pgwin32_install_crashdump_handler();
	#endif
	
	   /*
	    * 启动必要的子系统：错误和内存管理
	    *
	    * 从这一点开始的代码被允许使用elog/ereport进行错误和报告，尽管
	    * 消息的本地化可能不会立即工作，并且在GUC设置加载之前，消息不会
	    * 被发送到除了标准错误输出（stderr）之外的任何地方。
	    */
	    MemoryContextInit();
	
	    /*
	    * 从环境变量中设置区域信息。请注意，如果我们在一个已经初始化的数据库中，
	    * LC_CTYPE和LC_COLLATE将在之后被pg_control覆盖。我们在这里设置它们，以便
	    * 在initdb期间用于填充pg_control。LC_MESSAGES将在之后的GUC选项处理中设置，
	    * 但我们在这里设置它，以允许启动错误消息本地化。
	    */
	
	    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("postgres"));
	
	#ifdef WIN32
	
	    /*
	     * Windows使用代码页而不是环境变量，因此我们首先通过显式查询环境变量中的LC_COLLATE和LC_CTYPE来解决这个问题。
			 * 我们必须这样做，因为initdb会在环境中传递这些值。如果环境中没有这些值，我们就回退到使用代码页。
	     */
	    {
	        char       *env_locale;
	
	        if ((env_locale = getenv("LC_COLLATE")) != NULL)
	            init_locale("LC_COLLATE", LC_COLLATE, env_locale);
	        else
	            init_locale("LC_COLLATE", LC_COLLATE, "");
	
	        if ((env_locale = getenv("LC_CTYPE")) != NULL)
	            init_locale("LC_CTYPE", LC_CTYPE, env_locale);
	        else
	            init_locale("LC_CTYPE", LC_CTYPE, "");
	    }
	#else
	    init_locale("LC_COLLATE", LC_COLLATE, "");
	    init_locale("LC_CTYPE", LC_CTYPE, "");
	#endif
	
	#ifdef LC_MESSAGES
	    init_locale("LC_MESSAGES", LC_MESSAGES, "");
	#endif
	
	    /*
	     * We keep these set to "C" always, except transiently in pg_locale.c; see
	     * that file for explanations.
	     */
	    init_locale("LC_MONETARY", LC_MONETARY, "C");
	    init_locale("LC_NUMERIC", LC_NUMERIC, "C");
	    init_locale("LC_TIME", LC_TIME, "C");
	
	    /*
	     * 既然我们已经从区域设置环境中吸收了我们想要的所有信息，现在就移除任何LC_ALL的设置，
			 * 这样由pg_perm_setlocale安装的环境变量就会生效。
	     */
	    unsetenv("LC_ALL");
	
	    check_strxfrm_bug();
	
	    /*
	     * Catch standard options before doing much else, in particular before we
	     * insist on not being root.
	     */
	    if (argc > 1)
	    {
	        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
	        {
	            help(progname);
	            exit(0);
	        }
	        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
	        {
	            fputs(PG_BACKEND_VERSIONSTR, stdout);
	            exit(0);
	        }
	
	        /*
   				 * 除了上述情况外，我们允许root用户调用"--describe-config"和"-C var"。这是
				   * 相当安全的，因为这些是只读活动。-C选项的情况很重要，因为pg_ctl可能会
			     * 在Windows上仍然保持管理员权限的情况下尝试调用它。请注意，尽管-C选项通常可
				   * 以放在argv的任何位置，但如果你想绕过root用户检查，你必须把它放在第一位。这
			     * 减少了我们可能会误将其他模式的-C开关解释为postmaster/postgres的情况的风险。
    			*/
	        if (strcmp(argv[1], "--describe-config") == 0)
	            do_check_root = false;
	        else if (argc > 2 && strcmp(argv[1], "-C") == 0)
	            do_check_root = false;
	    }
	
	    /*
	     * 确保我们没有以root用户身份运行，除非对于所选选项这是安全的。
	     */
	    if (do_check_root)
	        check_root(progname);
	
	    /*
	     * 根据第一个参数调度到不同的子程序。
	     */
	
	#ifdef EXEC_BACKEND
	    if (argc > 1 && strncmp(argv[1], "--fork", 6) == 0)
	        SubPostmasterMain(argc, argv);    /* does not return */
	#endif
	
	#ifdef WIN32
			
			/*
	     * 启动我们的Win32信号实现
	     *
	     * SubPostmasterMain()会为自身执行这个操作，但其余的模式
	     * 需要在这里进行
	     */
	    pgwin32_signal_initialize();
	#endif
	
	    if (argc > 1 && strcmp(argv[1], "--boot") == 0)
	        AuxiliaryProcessMain(argc, argv);    /* does not return */
	    else if (argc > 1 && strcmp(argv[1], "--describe-config") == 0)
	        GucInfoMain();            /* does not return */
	    else if (argc > 1 && strcmp(argv[1], "--single") == 0)
	        PostgresMain(argc, argv,
	                     NULL,        /* no dbname */
	                     strdup(get_user_name_or_exit(progname)));    /* does not return */
	    else
	        PostmasterMain(argc, argv); /* does not return */
	    abort();                    /* should not get here */
	}
	
	
	
			/*
			 * 将平台特定的启动技巧放在这里。这是放置必须在任何新服务器进程启动初期执行的代码的
			 * 正确位置。请注意，当后端或子引导进程被派生（fork）时，除非我们处于fork/exec环境
			 * （即定义了EXEC_BACKEND），否则这段代码不会被执行。
			 *
			 * XXX 需要在这里添加代码证明了所涉及的平台过于愚蠢，无法提供标准的C执行环境而无需
			 * 额外帮助。如果可能的话，请避免在这里添加更多代码。
			 */
			static void
			startup_hacks(const char *progname)
			{
			    /*
			     * Windows特定执行环境的黑客攻击。
			     */
			#ifdef WIN32
		{
		    WSADATA        wsaData; // 用于存储Winsock版本信息的结构体
		    int            err;       // 用于存储WSAStartup函数的返回值
		
		    // 使标准输出和标准错误流默认为无缓冲区，这样可以立即显示输出内容
		    setvbuf(stdout, NULL, _IONBF, 0);
		    setvbuf(stderr, NULL, _IONBF, 0);
		
		    // 初始化Winsock，这是Windows平台上进行网络编程的必需步骤
		    err = WSAStartup(MAKEWORD(2, 2), &wsaData); // 请求版本2.2的Winsock支持
		    if (err != 0) // 如果初始化失败
		    {
		        write_stderr("%s: WSAStartup failed: %d\n", progname, err); // 将错误信息写入标准错误输出
		        exit(1); // 退出程序，返回错误代码1
		    }
		
		    // 设置错误模式，避免在发生一般保护故障时显示图形界面弹出框
		    // SEM_FAILCRITICALERRORS: 禁止显示由系统严重错误引起的对话框
		    // SEM_NOGPFAULTERRORBOX: 禁止显示由访问违规引起的对话框
		    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
		}
			#if defined(_M_AMD64) && _MSC_VER == 1800
			
			        /*----------
							 	* 如果使用MS Visual Studio 2013为x64编译并在支持AVX2指令集的CPU上运行Windows 7
					      * 之前版本（包括2008R2 SP1之前）的操作系统，避免在某些浮点运算操作中崩溃。
							 	*
							 	* 参考：https://connect.microsoft.com/VisualStudio/feedback/details/811093/visual-studio-2013-rtm-c-x64-code-generation-bug-for-avx2-instructions 
							 	*----------
							 	*/
			        if (!IsWindows7SP1OrGreater())
			        {
			            _set_FMA3_enable(0);
			        }
			#endif                            /* defined(_M_AMD64) && _MSC_VER == 1800 */
			
			    }
			#endif                            /* WIN32 */
			
			    /*
			     * 初始化dummy_spinlock，以防我们处于一个必须使用pg_memory_barrier()的后备实现
					 * 的平台上。
			     */
			    SpinLockInit(&dummy_spinlock);
			}
	
	
					/*
					 * 对一个区域设置类别进行初始的永久性设置。如果失败，
					 * 可能是由于环境中的LC_foo=invalid，那么就使用C语言的区域设置。如果连这也不行，
					 * 可能是由于内存不足，整个启动过程也会随之失败。
					 * 当这个函数返回时，我们保证已经为给定的
					 * 类别的环境变量设置了值。
					 */
			static void
			init_locale(const char *categoryname, int category, const char *locale)
			{
			    if (pg_perm_setlocale(category, locale) == NULL &&
			        pg_perm_setlocale(category, "C") == NULL)
			        elog(FATAL, "could not adopt \"%s\" locale nor C locale for %s",
			             locale, categoryname);
			}
			
			
	
				/*
				 * Help display should match the options accepted by PostmasterMain()
				 * and PostgresMain().
				 *
		 		 * XXX On Windows, non-ASCII localizations of these messages only display
				 * correctly if the console output code page covers the necessary characters.
				 * Messages emitted in write_console() do not exhibit this problem.
				 */
				/*
				 * help函数 - 打印PostgreSQL服务器的命令行选项帮助信息。
				 *
				 * 这个函数在用户请求帮助信息或者使用--help选项启动时被调用。
				 * 它提供了关于如何使用PostgreSQL命令行参数的详细说明，包括
				 * 一般选项、开发者选项、单用户模式选项、引导模式选项，以及节点选项
				 * (如果启用了PostgreSQL的扩展集群功能PGXC)。
				 */
		
		static void
		help(const char *progname)
		{
		    // 打印程序名称和基本用法
		    printf(_("%s is the PostgreSQL server.\n\n"), progname);
		    printf(_("Usage:\n  %s [OPTION]...\n\n"), progname);
		    printf(_("Options:\n"));
		
		    // 打印一般运行时选项
		    printf(_("  -B NBUFFERS        number of shared buffers\n")); // 设置共享缓冲区的数量
		    printf(_("  -c NAME=VALUE      set run-time parameter\n")); // 设置运行时参数
		    printf(_("  -C NAME            print value of run-time parameter, then exit\n")); // 打印运行时参数的值并退出
		    printf(_("  -d 1-5             debugging level\n")); // 设置调试级别
		    printf(_("  -D DATADIR         database directory\n")); // 设置数据库目录
		    printf(_("  -e                 use European date input format (DMY)\n")); // 使用欧洲日期输入格式（日月年）
		    printf(_("  -F                 turn fsync off\n")); // 关闭fsync以提高性能
		    printf(_("  -h HOSTNAME        host name or IP address to listen on\n")); // 设置监听的主机名或IP地址
		    printf(_("  -i                 enable TCP/IP connections\n")); // 启用TCP/IP连接
		    printf(_("  -k DIRECTORY       Unix-domain socket location\n")); // 设置Unix域套接字的位置
		#ifdef USE_SSL
		    // 如果编译时启用了SSL支持，则打印SSL相关选项
		    printf(_("  -l                 enable SSL connections\n"));
		#endif
		    printf(_("  -N MAX-CONNECT     maximum number of allowed connections\n")); // 设置允许的最大连接数
		    printf(_("  -o OPTIONS         pass \"OPTIONS\" to each server process (obsolete)\n")); // 将选项传递给每个服务器进程（已废弃）
		    printf(_("  -p PORT            port number to listen on\n")); // 设置监听的端口号
		    printf(_("  -s                 show statistics after each query\n")); // 在每个查询后显示统计信息
		    printf(_("  -S WORK-MEM        set amount of memory for sorts (in kB)\n")); // 设置用于排序的内存量（单位为kB）
		    printf(_("  -V, --version      output version information, then exit\n")); // 输出版本信息并退出
		    printf(_("  --NAME=VALUE       set run-time parameter\n")); // 设置运行时参数
		    printf(_("  --describe-config  describe configuration parameters, then exit\n")); // 描述配置参数并退出
		    printf(_("  -?, --help         show this help, then exit\n"));
		
		    // 打印开发者选项
		    printf(_("\nDeveloper options:\n"));
		    printf(_("  -f s|i|n|m|h       forbid use of some plan types\n")); // 禁止使用某些计划类型
		    printf(_("  -n                 do not reinitialize shared memory after abnormal exit\n")); // 异常退出后不重新初始化共享内存
		    printf(_("  -O                 allow system table structure changes\n")); // 允许系统表结构变化
		    printf(_("  -P                 disable system indexes\n")); // 禁用系统索引
		    printf(_("  -t pa|pl|ex        show timings after each query\n")); // 在每个查询后显示时间
		    printf(_("  -T                 send SIGSTOP to all backend processes if one dies\n")); // 如果一个后端进程死亡，向所有后端进程发送SIGSTOP信号
		    printf(_("  -W NUM             wait NUM seconds to allow attach from a debugger\n")); // 等待NUM秒以允许调试器附加
		    printf(_("  --localxid         use local transaction id (used only by initdb)\n")); // 使用本地事务ID（仅由initdb使用）
		
		    // 打印单用户模式选项
		    printf(_("\nOptions for single-user mode:\n"));
		    printf(_("  --single           selects single-user mode (must be first argument)\n")); // 选择单用户模式（必须是第一个参数）
		    printf(_("  DBNAME             database name (defaults to user name)\n")); // 设置数据库名称（默认为用户名）
		    printf(_("  -d 0-5             override debugging level\n")); // 覆盖调试级别
		    printf(_("  -E                 echo statement before execution\n")); // 在执行前回显语句
		    printf(_("  -j                 do not use newline as interactive query delimiter\n")); // 不使用换行符作为交互式查询分隔符
		    printf(_("  -r FILENAME        send stdout and stderr to given file\n")); // 将标准输出和标准错误重定向到指定文件
		
		    // 打印引导模式选项
		    printf(_("\nOptions for bootstrapping mode:\n"));
		    printf(_("  --boot             selects bootstrapping mode (must be first argument)\n")); // 选择引导模式（必须是第一个参数）
		    printf(_("  DBNAME             database name (mandatory argument in bootstrapping mode)\n")); // 设置数据库名称（引导模式下的强制性参数）
		    printf(_("  -r FILENAME        send stdout and stderr to given file\n")); // 将标准输出和标准错误重定向到指定文件
		    printf(_("  -x NUM             internal use\n")); // 内部使用
		
		#ifdef PGXC
		    // 如果启用了PGXC功能，则打印节点选项
		    printf(_("\nNode options:\n"));
		    printf(_("  --coordinator      start as a Coordinator\n")); // 作为协调节点启动
		    printf(_("  --datanode         start as a Datanode\n")); // 作为数据节点启动
		    printf(_("  --restoremode      start to restore existing schema on the new node to be added\n")); // 开始恢复要添加的新节点上的现有模式
		#endif
		
		    // 打印结束信息，引导用户查阅文档，并报告bug
		    printf(_("\nPlease read the documentation for the complete list of run-time\n"
		             "configuration settings and how to set them on the command line or in\n"
		             "the configuration file.\n\n"
		             "Report bugs to <pgsql-bugs@postgresql.org>.\n"));
		}
		
	
		static void
		check_root(const char *progname)
		{
		#ifndef WIN32
		    if (geteuid() == 0)
		    {
		        write_stderr("\"root\" execution of the PostgreSQL server is not permitted.\n"
		                     "The server must be started under an unprivileged user ID to prevent\n"
		                     "possible system security compromise.  See the documentation for\n"
		                     "more information on how to properly start the server.\n");
		        exit(1);
		    }
		
		    /*
		    * 还要确保实际用户ID和有效用户ID是相同的。从root shell执行setuid程序是一个安全隐患，
			* 因为在许多平台上，恶意的子程序可以将用户ID重新设置为root，如果实际用户ID是root的话。
		    * （由于实际上没有人将postgres作为setuid程序使用，因此主动解决这种情况似乎比它值得的
			* 麻烦更多；我们只是花费努力来检查这种情况。）
		    */
		    if (getuid() != geteuid())
		    {
		        write_stderr("%s: real and effective user IDs must match\n",
		                     progname);
		        exit(1);
		    }
		#else                            /* WIN32 */
		    if (pgwin32_is_admin())
		    {
		        write_stderr("Execution of PostgreSQL by a user with administrative permissions is not\n"
		                     "permitted.\n"
		                     "The server must be started under an unprivileged user ID to prevent\n"
		                     "possible system security compromises.  See the documentation for\n"
		                     "more information on how to properly start the server.\n");
		        exit(1);
		    }
		#endif                            /* WIN32 */
		}
