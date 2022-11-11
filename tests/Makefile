LIBPATH = /c/Users/X1/sortmergeAssign.jar
CLASSPATH = ../:$(LIBPATH):
JAVAC     = javac -classpath $(CLASSPATH)
JAVA      = java  -classpath $(CLASSPATH)

# test Sort Merge Join
SMTest:SM_JoinTest.java
	$(JAVAC) TestDriver.java QueryCheck.java SM_JoinTest.java

sortmerge: SMTest
	$(JAVA) tests.SM_JoinTest

xx:
	jar tf $(LIBPATH)

clean:
	\rm -f *.class *~ \#* core TRACE
