#Makefile (project)

#MULTITHREAD_SORT
#MULTITHREAD_QUERIES
#MULTITHREAD_SORT_CALL

CFLAGS=-g3 -O2 -Wall -Wchkp -DMULTITHREAD_QUERIES -DMULTITHREAD_SORT_CALL -D_FORTIFY_SOURCE=2 $(OPTFLAGS)
LDFLAGS=$(OPTLIBS)
PREFIX?=/usr/local

SOURCES=$(wildcard src/**/*.c src/*.c)
HEADERS=$(wildcard src/**/*.h src/*.h)
OBJECTS=$(patsubst %.c,%.o,$(SOURCES))

TEST_SRC=$(wildcard tests/*_main.c)
TESTS=$(patsubst %.c,%,$(TEST_SRC))

MAIN_SRC=$(wildcard main/*_main.c)

TARGET=build/queries.a
SO_TARGET=$(patsubst %.a,%.so,$(TARGET))

UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S), Linux)
	LDLIBS=-L./build -lm
endif

# The Target Build
all: $(TARGET) $(SO_TARGET) main

dev: CFLAGS=-g -Wall -Isrc -Wall $(OPTFLAGS)
dev: all

$(TARGET): CFLAGS += -fPIC
$(TARGET): build $(OBJECTS)
	ar rcs $@ $(OBJECTS)
	ranlib $@

$(SO_TARGET): $(TARGET) $(OBJECTS)
	$(CC) -shared -o queries $(OBJECTS)

build:
	@mkdir -p build
	@mkdir -p bin

# The Unit Tests
.PHONY: tests
main: $(TARGET)
	$(CC) $(CFLAGS) -o queries $(MAIN_SRC) $(TARGET) -lpthread

# The Cleaner
clean:
	rm -rf queries
	rm -rf build $(OBJECTS) $(TESTS)
	rm -f tests/tests.log
	find . -name "*.gc*" -exec rm {} \;
	rm -rf `find . -name "*.dSYM" -print`

count:
	wc -l src/*.c src/*.h main/*.c
