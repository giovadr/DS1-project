
.PHONY: compute_final_sum run_and_log compute_final_sum_with_java compile_java_check_program

compute_final_sum:
	@scripts/compute_final_sum.sh 'test.log'
	@cat 'test.log' | scripts/check_for_errors.sh

run_and_log:
	gradle run | tee 'test.log' | scripts/check_for_errors.sh

compile_java_check_program:
	javac check/Check.java

compute_final_sum_with_java:
	cd check && java Check '../test.log'

