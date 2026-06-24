/*
 * psql - the PostgreSQL interactive terminal
 *
 * Copyright (c) 2000-2017, PostgreSQL Global Development Group
 *
 * src/bin/psql/mainloop.c
 */
#include "postgres_fe.h"
#include "mainloop.h"

#include "command.h"
#include "common.h"
#include "input.h"
#include "prompt.h"
#include "settings.h"

#include "mb/pg_wchar.h"

/* opentenbase_ora compatible */
int	sql_mode;


/* callback functions for our flex lexer */
const PsqlScanCallbacks psqlscan_callbacks = {
	psql_get_variable,
	psql_error
};


static bool
ch_is_space(char ch)
{
	if (ch == ' ' || ch == '\n' || ch == '\t' || ch == '\r' || ch == '\f')
	{
	    return true;
	}
	else
	{
		return false;
	}
}

/*
 * Main processing loop for reading lines of input
 *	and sending them to the backend.
 *
 * This loop is re-entrant. May be called by \i command
 *	which reads input from a file.
 */
int
MainLoop(FILE *source)
{
	PsqlScanState scan_state;	/* lexer working state */
	ConditionalStack cond_stack;	/* \if status stack */
	volatile PQExpBuffer query_buf; /* buffer for query being accumulated */
	volatile PQExpBuffer previous_buf;	/* if there isn't anything in the new
										 * buffer yet, use this one for \e,
										 * etc. */
	PQExpBuffer history_buf;	/* earlier lines of a multi-line command, not
								 * yet saved to readline history */
	char	   *line;			/* current line of input */
	int			added_nl_pos;
	bool		success;
	bool		line_saved_in_history;
	volatile int successResult = EXIT_SUCCESS;
	volatile backslashResult slashCmdStatus = PSQL_CMD_UNKNOWN;
	volatile promptStatus_t prompt_status = PROMPT_READY;
	volatile int count_eof = 0;
	volatile bool die_on_error = false;
	FILE	   *prev_cmd_source;
	bool		prev_cmd_interactive;
	uint64		prev_lineno;
	char *tmp_char = NULL;
	int i = 0;
	bool need_psql_scan = false;
	char sql_start_cmd[12] = "";
	static int comment_len = 0;
	char *scan_pos = NULL;

	sql_mode = get_sql_mode();

	/* Save the prior command source */
	prev_cmd_source = pset.cur_cmd_source;
	prev_cmd_interactive = pset.cur_cmd_interactive;
	prev_lineno = pset.lineno;
	/* pset.stmt_lineno does not need to be saved and restored */

	/* Establish new source */
	pset.cur_cmd_source = source;
	pset.cur_cmd_interactive = ((source == stdin) && !pset.notty);
	pset.lineno = 0;
	pset.stmt_lineno = 1;

	/* Create working state */
	scan_state = psql_scan_create(&psqlscan_callbacks);
	cond_stack = conditional_stack_create();
	psql_scan_set_passthrough(scan_state, (void *) cond_stack);

	query_buf = createPQExpBuffer();
	previous_buf = createPQExpBuffer();
	history_buf = createPQExpBuffer();
	if (PQExpBufferBroken(query_buf) ||
		PQExpBufferBroken(previous_buf) ||
		PQExpBufferBroken(history_buf))
	{
		psql_error("out of memory\n");
		exit(EXIT_FAILURE);
	}

	/* main loop to get queries and execute them */
	while (successResult == EXIT_SUCCESS)
	{
		/*
		 * Clean up after a previous Control-C
		 */
		if (cancel_pressed)
		{
			if (!pset.cur_cmd_interactive)
			{
				/*
				 * You get here if you stopped a script with Ctrl-C.
				 */
				successResult = EXIT_USER;
				break;
			}

			cancel_pressed = false;
		}

		/*
		 * Establish longjmp destination for exiting from wait-for-input. We
		 * must re-do this each time through the loop for safety, since the
		 * jmpbuf might get changed during command execution.
		 */
		if (sigsetjmp(sigint_interrupt_jmp, 1) != 0)
		{
			/* got here with longjmp */

			/* reset parsing state */
			psql_scan_finish(scan_state);
			psql_scan_reset(scan_state);
			resetPQExpBuffer(query_buf);
			resetPQExpBuffer(history_buf);
			count_eof = 0;
			slashCmdStatus = PSQL_CMD_UNKNOWN;
			prompt_status = PROMPT_READY;
			pset.stmt_lineno = 1;
			cancel_pressed = false;

			if (pset.cur_cmd_interactive)
			{
				putc('\n', stdout);

				/*
				 * if interactive user is in an \if block, then Ctrl-C will
				 * exit from the innermost \if.
				 */
				if (!conditional_stack_empty(cond_stack))
				{
					psql_error("\\if: escaped\n");
					conditional_stack_pop(cond_stack);
				}
			}
			else
			{
				successResult = EXIT_USER;
				break;
			}
		}

		fflush(stdout);

		/*
		 * get another line
		 */
		if (pset.cur_cmd_interactive)
		{
			/* May need to reset prompt, eg after \r command */
			if (query_buf->len == 0)
				prompt_status = PROMPT_READY;
			line = gets_interactive(get_prompt(prompt_status, cond_stack),
									query_buf);
		}
		else
		{
#ifndef __OPTIMIZE__
			/*
			 * for opentenbase_test. the test framework start a psql process and interact with it. when psql can accept a new
			 * new line, it print a row delimiter, so the framework can know its status.
			 */
			if (pset.echo_row_delimiter)
			{
				fprintf(stdout, "b64eab59-6926-45a0-8e5b-fe5ca5926002-d5186d32ca207466062889283c40ff96-stdout\n");
				fflush(stdout);
				fprintf(stderr, "b64eab59-6926-45a0-8e5b-fe5ca5926002-d5186d32ca207466062889283c40ff96-stderr\n");
				fflush(stderr);
			}
#endif
			line = gets_fromFile(source);
			if (!line && ferror(source))
				successResult = EXIT_FAILURE;
		}

		/*
		 * query_buf holds query already accumulated.  line is the malloc'd
		 * new line of input (note it must be freed before looping around!)
		 */

		/* No more input.  Time to quit, or \i done */
		if (line == NULL)
		{
			if (pset.cur_cmd_interactive)
			{
				/* This tries to mimic bash's IGNOREEOF feature. */
				count_eof++;

				if (count_eof < pset.ignoreeof)
				{
					if (!pset.quiet)
						printf(_("Use \"\\q\" to leave %s.\n"), pset.progname);
					continue;
				}

				puts(pset.quiet ? "" : "\\q");
			}
			break;
		}

		count_eof = 0;

		pset.lineno++;

		/* ignore UTF-8 Unicode byte-order mark */
		if (pset.lineno == 1 && pset.encoding == PG_UTF8 && strncmp(line, "\xef\xbb\xbf", 3) == 0)
			memmove(line, line + 3, strlen(line + 3) + 1);

		/* Detect attempts to run custom-format dumps as SQL scripts */
		if (pset.lineno == 1 && !pset.cur_cmd_interactive &&
			strncmp(line, "PGDMP", 5) == 0)
		{
			free(line);
			puts(_("The input is a PostgreSQL custom-format dump.\n"
				   "Use the pg_restore command-line client to restore this dump to a database.\n"));
			fflush(stdout);
			successResult = EXIT_FAILURE;
			break;
		}

		/* no further processing of empty lines, unless within a literal */
		if (!pset.send_extra_lines && line[0] == '\0' && !psql_scan_in_quote(scan_state))
		{
			free(line);
			continue;
		}

		/* A request for help? Be friendly and give them some guidance */
		if (pset.cur_cmd_interactive && query_buf->len == 0 &&
			pg_strncasecmp(line, "help", 4) == 0 &&
			(line[4] == '\0' || line[4] == ';' || isspace((unsigned char) line[4])))
		{
			free(line);
			puts(_("You are using psql, the command-line interface to PostgreSQL."));
			printf(_("Type:  \\copyright for distribution terms\n"
					 "       \\h for help with SQL commands\n"
					 "       \\? for help with psql commands\n"
					 "       \\g or terminate with semicolon to execute query\n"
					 "       \\q to quit\n"));

			fflush(stdout);
			continue;
		}

		/* echo back if flag is set, unless interactive */
		if (pset.echo == PSQL_ECHO_ALL && !pset.cur_cmd_interactive)
		{
			puts(line);
			fflush(stdout);
		}

		/* insert newlines into query buffer between source lines */
		if (query_buf->len > 0)
		{
			appendPQExpBufferChar(query_buf, '\n');
			added_nl_pos = query_buf->len;
		}
		else
			added_nl_pos = -1;	/* flag we didn't add one */

		/* Setting this will not have effect until next line. */
		die_on_error = pset.on_error_stop;

		/*
		 * Parse line, looking for command separators.
		 */
		psql_scan_setup(scan_state, line, strlen(line),
						pset.encoding, standard_strings(),
						pset.send_extra_lines);
		success = true;
		line_saved_in_history = false;

		while (success || !die_on_error)
		{
			PsqlScanResult scan_result;
			promptStatus_t prompt_tmp = prompt_status;
			size_t		pos_in_query;
			char	   *tmp_line;

			pos_in_query = query_buf->len;
			/*
			 * begin opentenbase-ora-compatible
			 * non-opentenbase_ora mode always regarded as postgresql mode.
			 */
			if (ORA_MODE)
				scan_result = psql_scan_ora(scan_state, query_buf, &prompt_tmp);
			else
				scan_result = psql_scan(scan_state, query_buf, &prompt_tmp);
			/* end opentenbase-ora-compatible */

			prompt_status = prompt_tmp;

			if (PQExpBufferBroken(query_buf))
			{
				psql_error("out of memory\n");
				exit(EXIT_FAILURE);
			}

			/*
			 * Increase statement line number counter for each linebreak added
			 * to the query buffer by the last psql_scan() call. There only
			 * will be ones to add when navigating to a statement in
			 * readline's history containing newlines.
			 */
			tmp_line = query_buf->data + pos_in_query;
			while (*tmp_line != '\0')
			{
				if (*(tmp_line++) == '\n')
					pset.stmt_lineno++;
			}

			if (scan_result == PSCAN_EOL)
				pset.stmt_lineno++;

			/*
			 * Send command if semicolon found, or if end of line and we're in
			 * single-line mode.
			 */
			if (scan_result == PSCAN_SEMICOLON ||
				(scan_result == PSCAN_EOL && pset.singleline))
			{
				/*
				 * Save line in history.  We use history_buf to accumulate
				 * multi-line queries into a single history entry.  Note that
				 * history accumulation works on input lines, so it doesn't
				 * matter whether the query will be ignored due to \if.
				 */
				if (pset.cur_cmd_interactive && !line_saved_in_history)
				{
					pg_append_history(line, history_buf);
					pg_send_history(history_buf);
					line_saved_in_history = true;
				}

				/* execute query unless we're in an inactive \if branch */
				if (conditional_active(cond_stack))
				{
					success = SendQuery(query_buf->data);
					slashCmdStatus = success ? PSQL_CMD_SEND : PSQL_CMD_ERROR;
					pset.stmt_lineno = 1;

					/* transfer query to previous_buf by pointer-swapping */
					{
						PQExpBuffer swap_buf = previous_buf;

						previous_buf = query_buf;
						query_buf = swap_buf;
					}
					resetPQExpBuffer(query_buf);
					if (ORA_MODE)
						psql_scan_slash_reset(scan_state);
					added_nl_pos = -1;
					/* we need not do psql_scan_reset() here */
				}
				else
				{
					/* if interactive, warn about non-executed query */
					if (pset.cur_cmd_interactive)
						psql_error("query ignored; use \\endif or Ctrl-C to exit current \\if block\n");
					/* fake an OK result for purposes of loop checks */
					success = true;
					slashCmdStatus = PSQL_CMD_SEND;
					pset.stmt_lineno = 1;
					/* note that query_buf doesn't change state */
				}
			}
			else if (scan_result == PSCAN_BACKSLASH)
			{
				/* handle backslash command */

				/*
				 * If we added a newline to query_buf, and nothing else has
				 * been inserted in query_buf by the lexer, then strip off the
				 * newline again.  This avoids any change to query_buf when a
				 * line contains only a backslash command.  Also, in this
				 * situation we force out any previous lines as a separate
				 * history entry; we don't want SQL and backslash commands
				 * intermixed in history if at all possible.
				 */
				if (query_buf->len == added_nl_pos)
				{
					query_buf->data[--query_buf->len] = '\0';
					pg_send_history(history_buf);
				}
				added_nl_pos = -1;

				/* save backslash command in history */
				if (pset.cur_cmd_interactive && !line_saved_in_history)
				{
					pg_append_history(line, history_buf);
					pg_send_history(history_buf);
					line_saved_in_history = true;
				}

				/* execute backslash command */
				slashCmdStatus = HandleSlashCmds(scan_state,
												 cond_stack,
												 query_buf,
												 previous_buf);

				success = slashCmdStatus != PSQL_CMD_ERROR;

				/*
				 * Resetting stmt_lineno after a backslash command isn't
				 * always appropriate, but it's what we've done historically
				 * and there have been few complaints.
				 */
				pset.stmt_lineno = 1;

				if (slashCmdStatus == PSQL_CMD_SEND)
				{
					/* should not see this in inactive branch */
					Assert(conditional_active(cond_stack));

					success = SendQuery(query_buf->data);

					/* transfer query to previous_buf by pointer-swapping */
					{
						PQExpBuffer swap_buf = previous_buf;

						previous_buf = query_buf;
						query_buf = swap_buf;
					}
					resetPQExpBuffer(query_buf);

					/* flush any paren nesting info after forced send */
					psql_scan_reset(scan_state);
				}
				else if (slashCmdStatus == PSQL_CMD_NEWEDIT)
				{
					/* should not see this in inactive branch */
					Assert(conditional_active(cond_stack));
					/* rescan query_buf as new input */
					psql_scan_finish(scan_state);
					free(line);
					line = pg_strdup(query_buf->data);
					resetPQExpBuffer(query_buf);
					/* reset parsing state since we are rescanning whole line */
					psql_scan_reset(scan_state);
					psql_scan_setup(scan_state, line, strlen(line),
									pset.encoding, standard_strings(),
									pset.send_extra_lines);
					line_saved_in_history = false;
					prompt_status = PROMPT_READY;
				}
				else if (slashCmdStatus == PSQL_CMD_TERMINATE)
					break;
			}

			/* fall out of loop if lexer reached EOL */
			if (ORA_MODE)
			{
				char *query = query_buf->data;
				int   query_len = query_buf->len;
				bool  is_find = false;

				/* Turn up key to read all commands */
				tmp_char = NULL;
				memset(sql_start_cmd, 0, 12);

				query = skip_comments(query, query_len);
				if (query)
					query_len = strlen(query);

				is_find = false;
				query = skip_label(query,&is_find);
				if (query && is_find)
					query_len = strlen(query);

				query = skip_comments(query, query_len);
				if (query)
					query_len = strlen(query);
				
				if (query_len >= 8)
				{
					strncpy(sql_start_cmd, query, 7);
					for (i = 0; i < 7; i++)
					{
						sql_start_cmd[i] = tolower(sql_start_cmd[i]);
					}

					if (strncmp(sql_start_cmd, "declare", 7) == 0)
					{
						/* check sql start: declare */
						tmp_char = query + 7;
					}
					else if (strncmp(sql_start_cmd, "begin", 5) == 0)
					{
						/* check sql start: begin */
						tmp_char = query + 5;
					}
				}
				else if (query_len >= 6)
				{
					strncpy(sql_start_cmd, query, 5);
					for (i = 0; i < 5; i++)
					{
						sql_start_cmd[i] = tolower(sql_start_cmd[i]);
					}

					if (strncmp(sql_start_cmd, "begin", 5) == 0)
					{
						/* check sql start: begin */
						tmp_char = query + 5;
					}
				}

				if (tmp_char && (ch_is_space(*tmp_char) || (*tmp_char == ';')))
				{
					need_psql_scan = false;
					tmp_char = line;
					while (*tmp_char != '\0')
					{
						if (ch_is_space(*tmp_char))
						{
							tmp_char++;
						}
						else
						{
							break;
						}
					}

					if ((scan_result == PSCAN_INCOMPLETE) ||
						(scan_result == PSCAN_EOL && query_buf->len >= strlen(tmp_char)))
					{
						break;
					}
					else if (scan_result == PSCAN_EOL)
					{
						if (tmp_char[0] == '\\')
						{
							// process case : \set...,  \if...
							break;
						}

						if (tmp_char[0] == '-' && tmp_char[1] == '-')
						{
							// process case : comment '--' is row start (eg: select \n --comment info \n a,b...)
							i = 2;
							while (tmp_char[i] != '\0')
							{
								if (tmp_char[i] == '\n')
								{
									need_psql_scan = true;
									break;
								}
								i++;
							}

							if (!need_psql_scan)
								break;
						}

						if (tmp_char[query_buf->len] == '\\')
						{
							// process case : SELECT 2 \r ,  SELECT 3 \p ...
							break;
						}

						/* process case : comment '--' is row end (eg: begin \n select * from test; --comment info \n end;)
						 *
						 * tmp_char == "select * from test; --comment info..."
						 * OR
						 * tmp_char == "begin \n select * from test; --comment info \n end;"
						 *
						 * query_buf->data == "begin \n select * from test; "
						 */
						for (i = 0; i < query_buf->len && i < 7; i++)
						{
							if (query_buf->data[i] != tmp_char[i])
							{
								break;
							}
						}
						if (i == 7 || i == query_buf->len)
						{
							char *comment_start = NULL;
							/*
							 * tmp_char == "begin \n select * from test; --comment info \n update ... ; --comment \n end;"
							 * query_buf->data == "begin \n select * from test; "
							 */

							scan_pos = tmp_char + query_buf->len + comment_len;
							while (*scan_pos != '\0')
							{
								if (*scan_pos == '-' && *(scan_pos + 1) == '-')
								{
									comment_start = scan_pos;
								}

								if (comment_start && tolower(*scan_pos) == '\n')
								{
									comment_len = comment_len + scan_pos - comment_start + 1;
									break;
								}

								if (tolower(*scan_pos) == '\n')
									break;

								scan_pos++;
							}

							if(comment_start && *scan_pos == '\0')
							{
								comment_len = comment_len + scan_pos - comment_start + 1;
							}

							if ((query_buf->len + comment_len) < strlen(tmp_char))
							{
								need_psql_scan = true;
							}

							if (!need_psql_scan)
							{
								comment_len = 0;
								break;
							}
						}
						else
						{
							/*
							 * tmp_char == "select * from test; --comment info..."
							 * query_buf->data == "begin \n select * from test; "
							 */
							break;
						}
					}
				}
				else if (scan_result == PSCAN_INCOMPLETE ||
						 scan_result == PSCAN_EOL)
					break;
			}
			else
			{
				if (scan_result == PSCAN_INCOMPLETE ||
					scan_result == PSCAN_EOL)
					break;
			}
		}

		/* Add line to pending history if we didn't execute anything yet */
		if (pset.cur_cmd_interactive && !line_saved_in_history)
			pg_append_history(line, history_buf);

		psql_scan_finish(scan_state);
		free(line);

		if (slashCmdStatus == PSQL_CMD_TERMINATE)
		{
			successResult = EXIT_SUCCESS;
			break;
		}

		if (!pset.cur_cmd_interactive)
		{
			if (!success && die_on_error)
				successResult = EXIT_USER;
			/* Have we lost the db connection? */
			else if (!pset.db)
				successResult = EXIT_BADCONN;
		}
	}							/* while !endoffile/session */

	/*
	 * Process query at the end of file without a semicolon
	 */
	if (query_buf->len > 0 && !pset.cur_cmd_interactive &&
		successResult == EXIT_SUCCESS)
	{
		/* save query in history */
		if (pset.cur_cmd_interactive)
			pg_send_history(history_buf);

		/* execute query unless we're in an inactive \if branch */
		if (conditional_active(cond_stack))
		{
			success = SendQuery(query_buf->data);
			if (ORA_MODE)
				psql_scan_slash_reset(scan_state);
		}
		else
		{
			if (pset.cur_cmd_interactive)
				psql_error("query ignored; use \\endif or Ctrl-C to exit current \\if block\n");
			success = true;
		}

		if (!success && die_on_error)
			successResult = EXIT_USER;
		else if (pset.db == NULL)
			successResult = EXIT_BADCONN;
	}

	/*
	 * Check for unbalanced \if-\endifs unless user explicitly quit, or the
	 * script is erroring out
	 */
	if (slashCmdStatus != PSQL_CMD_TERMINATE &&
		successResult != EXIT_USER &&
		!conditional_stack_empty(cond_stack))
	{
		psql_error("reached EOF without finding closing \\endif(s)\n");
		if (die_on_error && !pset.cur_cmd_interactive)
			successResult = EXIT_USER;
	}

	/*
	 * Let's just make real sure the SIGINT handler won't try to use
	 * sigint_interrupt_jmp after we exit this routine.  If there is an outer
	 * MainLoop instance, it will reset sigint_interrupt_jmp to point to
	 * itself at the top of its loop, before any further interactive input
	 * happens.
	 */
	sigint_interrupt_enabled = false;

	destroyPQExpBuffer(query_buf);
	destroyPQExpBuffer(previous_buf);
	destroyPQExpBuffer(history_buf);

	psql_scan_destroy(scan_state);
	conditional_stack_destroy(cond_stack);

	pset.cur_cmd_source = prev_cmd_source;
	pset.cur_cmd_interactive = prev_cmd_interactive;
	pset.lineno = prev_lineno;

	return successResult;
}								/* MainLoop() */
