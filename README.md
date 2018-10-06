# zeppelin-q-interpreter
Zeppelin: KDB/Q interpreter

Warning: This code is more a proof of concept than real work.

However It works good enough, if you are just looking for executing q scripts on a paragraph. 
* Kdb seems to drop open connections after several hours. So the Interpreter open an close connection to Kdb on each paragraph execution. The consequence is you cannot share variables from one paragraph to another.
* The Interpreter will execute q scripts row by row, so if a row is commented (starts by "/") it will be ignored.
* No Cancel implemented yet.
* In Zeppelin version 0.6 it was possible to share variables between Q and another language (Java, Python ..) but same code does not work anymore in Zeppelin version 0.7.

How to Install?
1. Execute mvn install to build the jar
2. Follow instructions from Zeppelin website to install a new interpreter
