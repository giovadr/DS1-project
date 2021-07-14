
.PHONY: compute_final_sum run_and_log compute_final_sum_with_java compile_java_check_program

compute_final_sum:
	./compute_final_sum.sh 'test.log'

run_and_log:
	gradle run > 'test.log'

compute_final_sum_with_java:
	cd check && java Check '../test.log'

compile_java_check_program:
	javac check/Check.java


