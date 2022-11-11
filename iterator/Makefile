LIBPATH = /c/Users/X1/sortmergeAssign.jar
CLASSPATH = ../:$(LIBPATH):
JAVAC     = javac -classpath $(CLASSPATH)
JAVA      = java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

xx:
	jar tf $(LIBPATH)

clean:
	\rm -f *.class *~ \#* core 
