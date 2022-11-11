LIBPATH = /eecs/dept/course/2022-23/F/4411/Project/mj/lib/sortmergeAssign.jar
CLASSPATH = ../:$(LIBPATH):
JAVAC     = /eecs/local/bin/javac -classpath $(CLASSPATH)
JAVA      = /eecs/local/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

xx:
	jar tf $(LIBPATH)

clean:
	\rm -f *.class *~ \#* core 
