BEGIN {
	FS = ","
}

FNR == 1 {
	if (NF != 15 || $15 != "result_count")
		schema_errors++
	next
}

{
	if (NF != 15)
	{
		malformed_rows++
		next
	}

	key = $2 SUBSEP $3 SUBSEP $4 SUBSEP $5 SUBSEP $6
	if (ARGIND == 1)
	{
		if ((key in base_hash) && base_hash[key] != $9)
			repeat_hash_mismatches++
		if ((key in base_result_count) && base_result_count[key] != $15)
			repeat_result_count_mismatches++
		keys[key] = 1
		base_recall[key] += $8
		base_p50[key] = base_p50[key] " " $10
		base_p95[key] = base_p95[key] " " $11
		base_qps[key] = base_qps[key] " " $12
		base_hash[key] = $9
		base_result_count[key] = $15
		base_count[key]++
	}
	else
	{
		if ((key in opt_hash) && opt_hash[key] != $9)
			repeat_hash_mismatches++
		if ((key in opt_result_count) && opt_result_count[key] != $15)
			repeat_result_count_mismatches++
		keys[key] = 1
		opt_recall[key] += $8
		opt_p50[key] = opt_p50[key] " " $10
		opt_p95[key] = opt_p95[key] " " $11
		opt_qps[key] = opt_qps[key] " " $12
		opt_hash[key] = $9
		opt_result_count[key] = $15
		opt_count[key]++
		profile[key] = $2
		metric[key] = $3
		lists[key] = $4
		probes[key] = $5
		clients[key] = $6
	}
}

function median(text, values, count, i, j, tmp)
{
	count = split(text, values, / +/)
	if (values[1] == "")
	{
		for (i = 1; i < count; i++)
			values[i] = values[i + 1]
		count--
	}
	for (i = 1; i <= count; i++)
		for (j = i + 1; j <= count; j++)
			if (values[j] + 0 < values[i] + 0)
			{
				tmp = values[i]
				values[i] = values[j]
				values[j] = tmp
			}
	return values[int((count + 1) / 2)] + 0
}

function delta(new_value, old_value)
{
	if (old_value == 0)
		return 0
	return (new_value - old_value) * 100.0 / old_value
}

END {
	PROCINFO["sorted_in"] = "@ind_str_asc"
	matched = 0
	incomplete = 0
	hash_mismatches = 0
	result_count_mismatches = 0
	worst_recall_drop_pp = 0
	worst_p95_delta = -1e30
	worst_qps_delta = 1e30
	best_p95_delta = 1e30
	best_qps_delta = -1e30
	print "| Profile | Metric | Lists | Probes | Clients | Recall base/opt | Results base/opt | Hash | P50 ms base/opt | P95 ms base/opt | QPS base/opt | P95 delta | QPS delta |"
	print "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
	for (key in keys)
	{
		if (!(key in base_count) || !(key in opt_count) || base_count[key] != opt_count[key])
		{
			incomplete++
			continue
		}
		br = base_recall[key] / base_count[key]
		orr = opt_recall[key] / opt_count[key]
		bp50 = median(base_p50[key])
		op50 = median(opt_p50[key])
		bp95 = median(base_p95[key])
		op95 = median(opt_p95[key])
		bqps = median(base_qps[key])
		oqps = median(opt_qps[key])
		hash_status = (base_hash[key] == opt_hash[key] ? "same" : "DIFF")
		result_count_status = (base_result_count[key] == opt_result_count[key] ? "same" : "DIFF")
		p95_delta = delta(op95, bp95)
		qps_delta = delta(oqps, bqps)
		recall_drop_pp = (orr - br) * 100.0
		matched++
		if (hash_status != "same")
			hash_mismatches++
		if (result_count_status != "same")
			result_count_mismatches++
		if (recall_drop_pp < worst_recall_drop_pp)
			worst_recall_drop_pp = recall_drop_pp
		if (p95_delta > worst_p95_delta)
			worst_p95_delta = p95_delta
		if (qps_delta < worst_qps_delta)
			worst_qps_delta = qps_delta
		if (p95_delta < best_p95_delta)
			best_p95_delta = p95_delta
		if (qps_delta > best_qps_delta)
			best_qps_delta = qps_delta
		printf "| %s | %s | %d | %d | %d | %.4f / %.4f | %d / %d | %s | %.3f / %.3f | %.3f / %.3f | %.1f / %.1f | %+.2f%% | %+.2f%% |\n", \
			profile[key], metric[key], lists[key], probes[key], clients[key], br, orr, \
			base_result_count[key], opt_result_count[key], hash_status, \
			bp50, op50, bp95, op95, bqps, oqps, p95_delta, qps_delta
	}

	correctness_ok = (matched > 0 && incomplete == 0 && hash_mismatches == 0 &&
		result_count_mismatches == 0 && repeat_hash_mismatches == 0 &&
		repeat_result_count_mismatches == 0 && malformed_rows == 0 && schema_errors == 0)
	recall_ok = (matched > 0 && worst_recall_drop_pp >= -0.1)
	regression_ok = (matched > 0 && worst_p95_delta <= 3.0 && worst_qps_delta >= -3.0)
	improvement_ok = (matched > 0 && (best_p95_delta <= -10.0 || best_qps_delta >= 10.0))

	print ""
	print "## Gate summary"
	printf "\n- Result stability: %s (%d matched configurations, %d cross-build hash mismatches, %d result-count mismatches, %d incomplete).\n", correctness_ok ? "PASS" : "FAIL", matched, hash_mismatches, result_count_mismatches, incomplete
	printf "- Input integrity: %s (%d schema errors, %d malformed rows, %d repeat hash mismatches, %d repeat result-count mismatches).\n", (schema_errors == 0 && malformed_rows == 0 && repeat_hash_mismatches == 0 && repeat_result_count_mismatches == 0) ? "PASS" : "FAIL", schema_errors, malformed_rows, repeat_hash_mismatches, repeat_result_count_mismatches
	printf "- Recall stability: %s (worst change %+.3f percentage points; limit -0.100).\n", recall_ok ? "PASS" : "FAIL", worst_recall_drop_pp
	printf "- Regression guard: %s (worst P95 %+.2f%%, worst QPS %+.2f%%; limit 3%%).\n", regression_ok ? "PASS" : "FAIL", worst_p95_delta, worst_qps_delta
	printf "- Improvement: %s (best P95 %+.2f%%, best QPS %+.2f%%; target 10%%).\n", improvement_ok ? "PASS" : "FAIL", best_p95_delta, best_qps_delta

	if (!correctness_ok || !recall_ok || !regression_ok || !improvement_ok)
		exit 1
}
