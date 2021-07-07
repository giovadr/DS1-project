
.PHONY: compute_final_sum run_and_log

compute_final_sum:
	./compute_final_sum.sh 'test.log'

run_and_log:
	gradle run > 'test.log'



