# DS1-project
Project for the course "Distributed Systems 1"

## Check implementation correctness

To run the main program, execute:
```
gradle run > test.log
```

After starting the program, let it run for some time and then press the `Enter` key.

To compile the `Check.java` file, execute:
```
javac check/Check.java
```

To check that the program is correct, execute:
```
cd check && java Check '../test.log'
```

The total sum should be equal to the number of servers multiplied by 1000.
