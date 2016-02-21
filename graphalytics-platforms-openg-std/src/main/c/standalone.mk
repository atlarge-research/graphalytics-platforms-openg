## Generate a symlink in current directory to graphbig 
GRAPHBIG_ROOT=./graphbig

UNIT_TEST_TARGETS=bfs cdlp lcc pr sssp wcc

CXX_FLAGS+=-std=c++0x -Wall -Wno-deprecated
INCLUDE+=-I${GRAPHBIG_ROOT}/common -I${GRAPHBIG_ROOT}/openG -I${GRAPHBIG_ROOT}/csr_bench/lib
EXTRA_CXX_FLAGS+=-L${GRAPHBIG_ROOT}/tools/lib

OUTPUT_LOG=output.log

LIBS=$(EXTRA_LIBS)

ifeq (${TEST}, 1)
  CXX_FLAGS += -DGRANULA
endif

ifeq (${PFM},0)
  CXX_FLAGS += -DNO_PFM
else
  PERF_LIBS += -lpfm_cxx -lpfm
  INCLUDE += -I${GRAPHBIG_ROOT}/tools/include
endif

ifeq (${PERF},0)
  CXX_FLAGS += -DNO_PERF
  PERF_LIBS=
endif

## use openmp
CXX_FLAGS += -DUSE_OMP
EXTRA_CXX_FLAGS+=-fopenmp

ifeq (${DEBUG},1)
  CXX_FLAGS += -DDEBUG -g
else
  CXX_FLAGS +=-O3
endif

ifeq (${VERIFY},1)
  CXX_FLAGS += -DENABLE_VERIFY
endif

ifeq (${OUTPUT}, 1)
  EXTRA_CXX_FLAGS+=-DENABLE_OUTPUT
endif

EXTRA_LIBS+=${PERF_LIBS}
CXX_FLAGS+=$(EXTRA_CXX_FLAGS) $(INCLUDE)
LINKER_OPTIONS=$(CXX_FLAGS)
ALL_TARGETS=${UNIT_TEST_TARGETS}

all: ${ALL_TARGETS}

.cc.o:
	${CXX} -c ${CXX_FLAGS} $<

.cpp.o:
	${CXX} -c ${CXX_FLAGS} $<


${UNIT_TEST_TARGETS}:
	${CXX} ${CXX_FLAGS} ${LIBS} -o $@ $@.cpp $(LIBS)


run: ${UNIT_TEST_TARGETS}
	@for i in ${UNIT_TEST_TARGETS}; do  \
		echo "Running $$i, output in $$i.log"; \
		./$$i --dataset ./test  --output $$i.out > $$i.log 2>&1; \
	done	

clean:
	@-/bin/rm -rf ${ALL_TARGETS} ${GENERATED_DIRS} *.o *~ core core.* 
