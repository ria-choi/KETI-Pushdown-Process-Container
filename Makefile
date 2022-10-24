CC=g++
CFLAGS=-g -Wall
OBJS=StorageEngineInputInterface.o TableManager.o CSDScheduler.o buffer_manager.o CSDManager.o SnippetManager.o mergequerykmc.o
TARGET=a.out
 
$(TARGET): $(OBJS) 
	$(CC)	-o	$@	$(OBJS) -lpthread -lboost_thread -lgrpc -std=c++17

StorageEngineInputInterface.o: TableManager.h keti_type.h StorageEngineInputInterface.cc buffer_manager.cc mergequerykmc.h SnippetManager.h CSDManager.h snippet_sample.grpc.pb.h
TableManager.o: TableManager.h TableManager.cc
CSDScheduler.o: buffer_manager.h CSDScheduler.cc
buffer_manager.o: buffer_manager.h buffer_manager.cc
CSDManager.o: CSDManager.h CSDManager.cc
SnippetManager.o : SnippetManager.h SnippetManager.cc
mergequerykmc.o: mergequerykmc.h mergequerykmc.cc

clean:
	rm -f *.o