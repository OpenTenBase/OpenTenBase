{
	values[NR] = $1
}

END {
	if (NR == 0)
		exit 1
	p50 = int((NR - 1) * 0.50) + 1
	p95 = int((NR - 1) * 0.95) + 1
	printf "%.3f,%.3f\n", values[p50] / 1000.0, values[p95] / 1000.0
}
